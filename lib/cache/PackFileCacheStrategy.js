/*
	MIT License http://www.opensource.org/licenses/mit-license.php
	Author Tobias Koppers @sokra
*/

"use strict";

const FileSystemInfo = require("../FileSystemInfo");
const LazySet = require("../util/LazySet");
const makeSerializable = require("../util/makeSerializable");
const memorize = require("../util/memorize");
const { createFileSerializer } = require("../util/serialization");

/** @typedef {import("../Cache").Etag} Etag */
/** @typedef {import("../logging/Logger").Logger} Logger */
/** @typedef {import("../util/fs").IntermediateFileSystem} IntermediateFileSystem */

class PackContainer {
	/**
	 * @param {Object} data stored data
	 * @param {string} version version identifier
	 * @param {FileSystemInfo.Snapshot} buildSnapshot snapshot of all build dependencies
	 * @param {Set<string>} buildDependencies list of all unresolved build dependencies captured
	 * @param {Map<string, string>} resolveResults result of the resolved build dependencies
	 * @param {FileSystemInfo.Snapshot} resolveBuildDependenciesSnapshot snapshot of the dependencies of the build dependencies resolving
	 */
	constructor(
		data,
		version,
		buildSnapshot,
		buildDependencies,
		resolveResults,
		resolveBuildDependenciesSnapshot
	) {
		this.data = data;
		this.version = version;
		this.buildSnapshot = buildSnapshot;
		this.buildDependencies = buildDependencies;
		this.resolveResults = resolveResults;
		this.resolveBuildDependenciesSnapshot = resolveBuildDependenciesSnapshot;
	}

	serialize({ write, writeLazy }) {
		write(this.version);
		write(this.buildSnapshot);
		write(this.buildDependencies);
		write(this.resolveResults);
		write(this.resolveBuildDependenciesSnapshot);
		writeLazy(this.data);
	}

	deserialize({ read }) {
		this.version = read();
		this.buildSnapshot = read();
		this.buildDependencies = read();
		this.resolveResults = read();
		this.resolveBuildDependenciesSnapshot = read();
		this.data = read();
	}
}

makeSerializable(
	PackContainer,
	"webpack/lib/cache/PackFileCacheStrategy",
	"PackContainer"
);

const MIN_CONTENT_SIZE = 2 * 1024 * 1024;
const FILES_PER_LEVEL = 10;
const MAX_AGE = 1000 * 60 * 60 * 24 * 60; // 1 month

class Pack {
	constructor(logger) {
		/** @type {Map<string, string | null>} */
		this.etags = new Map();
		/** @type {Map<string, number>} */
		this.locations = new Map();
		/** @type {Map<string, any>} */
		this.freshContent = new Map();
		/** @type {(undefined | PackContent)[]} */
		this.content = [];
		this.lastAccess = new Map();
		this.unserializable = new Set();
		this.used = new Set();
		this.invalid = false;
		this.logger = logger;
	}

	/**
	 * @param {string} identifier unique name for the resource
	 * @param {string | null} etag etag of the resource
	 * @returns {any} cached content
	 */
	get(identifier, etag) {
		const etagInCache = this.etags.get(identifier);
		if (etagInCache === undefined) return undefined;
		if (etagInCache !== etag) return null;
		this.used.add(identifier);

		// Find data entry
		const loc = this.locations.get(identifier);
		if (loc === -1) {
			return this.freshContent.get(identifier);
		} else {
			if (!this.content[loc]) {
				return undefined;
			}
			return this.content[loc].get(identifier);
		}
	}

	/**
	 * @param {string} identifier unique name for the resource
	 * @param {string | null} etag etag of the resource
	 * @param {any} data cached content
	 * @returns {void}
	 */
	set(identifier, etag, data) {
		if (this.unserializable.has(identifier)) return;
		this.used.add(identifier);
		if (!this.invalid) {
			this.invalid = true;
			this.logger.debug(`Pack got invalid because of ${identifier}`);
		}
		this.etags.set(identifier, etag);
		this.freshContent.set(identifier, data);
		const loc = this.locations.get(identifier);
		if (loc >= 0) {
			const content = this.content[loc];
			content.delete(identifier);
			if (content.items.size === 0) {
				this.content[loc] = undefined;
			}
		}
		this.locations.set(identifier, -1);
	}

	/**
	 * @param {Set<string>} items data keys
	 * @param {Set<string>} usedItems items marked as used
	 * @param {Map<string, any> | function(): Promise<Map<string, any>>} map data entries
	 * @param {number} level level where to put the data
	 * @returns {number} new location of data entries
	 */
	_putContent(items, usedItems, map, level) {
		/** @type {[PackContent, number][]} */
		const existingContent = [];
		const startIndex = level * FILES_PER_LEVEL;
		const endIndex = (level + 1) * FILES_PER_LEVEL;
		for (let i = startIndex; i < endIndex; i++) {
			if (this.content[i] === undefined) {
				// Empty space, we can put it here
				this.content[i] = new PackContent(items, usedItems, map);
				return i;
			}
			existingContent.push([this.content[i], i]);
		}
		// No empty space, try to merge all small contents, or all contents
		const smallExistingContent = existingContent.filter(([c]) => {
			const size = c.getSize();
			return size > 0 && size < MIN_CONTENT_SIZE;
		});
		const selectedContent =
			smallExistingContent.length > 2 ? smallExistingContent : existingContent;
		const newLoc = selectedContent[0][1];
		const insertLoc = selectedContent[1][1];
		const mergedContent = selectedContent.map(([c]) => c);

		// 1. Put final content into this level
		for (const [, i] of selectedContent) {
			this.content[i] = undefined;
		}
		this.content[insertLoc] = new PackContent(items, usedItems, map);

		// 2. Get list of items in the existing content
		// while on it garbage collect old items
		const now = Date.now();
		/** @type {Set<string>} */
		const mergedItems = new Set();
		/** @type {Set<string>} */
		const mergedUsedItems = new Set();
		for (const content of mergedContent) {
			for (const identifier of content.items) {
				if (
					!this.used.has(identifier) &&
					now - this.lastAccess.get(identifier) > MAX_AGE
				) {
					this.lastAccess.delete(identifier);
					this.etags.delete(identifier);
					this.locations.delete(identifier);
				} else {
					mergedItems.add(identifier);
				}
			}
			for (const identifer of content.used) {
				mergedUsedItems.add(identifer);
			}
		}

		if (mergedItems.size > 0) {
			// 3. Insert (lazy) merged content into the next level
			this.content[newLoc] = new PackContent(
				mergedItems,
				mergedUsedItems,
				memorize(async () => {
					// unpack existing content in parallel
					// after that values are accessible in .content
					await Promise.all(mergedContent.map(c => c.unpack()));
					/** @type {Map<string, any>} */
					const map = new Map();
					for (const content of mergedContent) {
						for (const [identifier, value] of content.content) {
							if (mergedItems.has(identifier)) {
								map.set(identifier, value);
							}
						}
					}
					return map;
				})
			);

			// 4. update location of these items
			for (const identifier of mergedItems) {
				this.locations.set(identifier, newLoc);
			}
		}

		return startIndex;
	}

	_updateLastAccess() {
		const now = Date.now();
		for (const identifier of this.used) {
			this.lastAccess.set(identifier, now);
		}
		this.used.clear();
	}

	_persistFreshContent() {
		if (this.freshContent.size > 0) {
			const items = new Set(this.freshContent.keys());
			const map = new Map(this.freshContent);
			const loc = this._putContent(items, new Set(items), map, 0);
			for (const identifier of items) {
				this.locations.set(identifier, loc);
			}
			this.freshContent.clear();
		}
	}

	_optimizeUnusedContent() {
		for (let i = 0; i < this.content.length; i++) {
			const content = this.content[i];
			if (content === undefined) continue;
			const size = content.getSize();
			if (size < MIN_CONTENT_SIZE) continue;
			const used = content.used.size;
			const total = content.items.size;
			if (used > 0 && used < total) {
				// remove this content
				this.content[i] = undefined;

				const level = Math.floor(i / FILES_PER_LEVEL);
				const usedItems = new Set(content.used);
				const newLoc = this._putContent(
					usedItems,
					new Set(usedItems),
					async () => {
						await content.unpack();
						const map = new Map();
						for (const identifier of usedItems) {
							map.set(identifier, content.content.get(identifier));
						}
						return map;
					},
					level + 1
				);
				const unusedItems = new Set(content.items);
				for (const identifier of usedItems) {
					this.locations.set(identifier, newLoc);
					unusedItems.delete(identifier);
				}
				const newUnusedLoc = this._putContent(
					unusedItems,
					new Set(),
					async () => {
						await content.unpack();
						const map = new Map();
						for (const identifier of unusedItems) {
							map.set(identifier, content.content.get(identifier));
						}
						return map;
					},
					level + 1
				);
				for (const identifier of unusedItems) {
					this.locations.set(identifier, newUnusedLoc);
				}

				// optimizing only one of them is good enough and
				// reduces the amount of serialization needed
				return;
			}
		}
	}

	serialize({ write, writeFile }) {
		this._updateLastAccess();
		this._persistFreshContent();
		this._optimizeUnusedContent();
		write(this.etags);
		write(this.unserializable);
		write(this.lastAccess);
		for (let i = 0; i < this.content.length; i++) {
			const content = this.content[i];
			if (content !== undefined) {
				write(content.items);
				writeFile(content.getMap(), { name: `${i}` });
			} else {
				write(undefined);
			}
		}
		write(null);
	}

	deserialize({ read, logger }) {
		this.logger = logger;
		this.etags = read();
		this.unserializable = read();
		// Mark the first 1% of the items for retry
		// If they still fail they are added to the back again
		// Eventually all items are retried, but not too many at the same time
		if (this.unserializable.size > 0) {
			let i = Math.ceil(this.unserializable.size / 100);
			for (const item of this.unserializable) {
				this.unserializable.delete(item);
				if (--i <= 0) break;
			}
		}
		this.lastAccess = read();
		this.locations.clear();
		this.content.length = 0;
		let items = read();
		while (items !== null) {
			if (items === undefined) {
				this.content.push(items);
			} else {
				const idx = this.content.length;
				const lazy = read();
				this.content.push(new PackContent(items, new Set(), lazy));
				for (const identifier of items) {
					this.locations.set(identifier, idx);
				}
			}
			items = read();
		}
	}
}

makeSerializable(Pack, "webpack/lib/cache/PackFileCacheStrategy", "Pack");

class PackContent {
	/**
	 * @param {Set<string>} items keys
	 * @param {Set<string>} usedItems used keys
	 * @param {Map<string, any> | function(): Promise<Map<string, any>>} dataOrFn sync or async content
	 */
	constructor(items, usedItems, dataOrFn) {
		this.items = items;
		/** @type {function(): Map<string, any> | Promise<Map<string, any>>} */
		this.lazy = typeof dataOrFn === "function" ? dataOrFn : undefined;
		/** @type {Map<string, any>} */
		this.content = typeof dataOrFn === "function" ? undefined : dataOrFn;
		this.outdated = false;
		this.used = usedItems;
	}

	get(identifier) {
		this.used.add(identifier);
		if (this.content) {
			return this.content.get(identifier);
		}
		const value = this.lazy();
		if (value instanceof Promise) {
			return value.then(data => {
				this.content = data;
				return data.get(identifier);
			});
		} else {
			this.content = value;
			return value.get(identifier);
		}
	}

	/**
	 * @returns {void | Promise} maybe a promise if lazy
	 */
	unpack() {
		if (this.content) return;
		if (this.lazy) {
			const value = this.lazy();
			if (value instanceof Promise) {
				return value.then(data => {
					this.content = data;
				});
			} else {
				this.content = value;
			}
		}
	}

	/**
	 * @returns {number} size of the content or -1 if not known
	 */
	getSize() {
		if (!this.lazy) return -1;
		const options = /** @type {any} */ (this.lazy).options;
		if (!options) return -1;
		const size = options.size;
		if (typeof size !== "number") return -1;
		return size;
	}

	delete(identifier) {
		this.items.delete(identifier);
		this.used.delete(identifier);
		this.outdated = true;
	}

	/**
	 * @returns {function(): Map<string, any> | Promise<Map<string, any>>} lazy map
	 */
	getMap() {
		if (!this.outdated && this.lazy) return this.lazy;
		if (!this.outdated && this.content) {
			const map = new Map(this.content);
			return (this.lazy = () => map);
		}
		this.outdated = false;
		if (this.content) {
			return (this.lazy = memorize(() => {
				/** @type {Map<string, any>} */
				const map = new Map();
				for (const item of this.items) {
					map.set(item, this.content.get(item));
				}
				return map;
			}));
		}
		const lazy = this.lazy;
		return (this.lazy = () => {
			const value = lazy();
			if (value instanceof Promise) {
				return value.then(data => {
					/** @type {Map<string, any>} */
					const map = new Map();
					for (const item of this.items) {
						map.set(item, data.get(item));
					}
					return map;
				});
			} else {
				/** @type {Map<string, any>} */
				const map = new Map();
				for (const item of this.items) {
					map.set(item, value.get(item));
				}
				return map;
			}
		});
	}
}

class PackFileCacheStrategy {
	/**
	 * @param {Object} options options
	 * @param {IntermediateFileSystem} options.fs the filesystem
	 * @param {string} options.context the context directory
	 * @param {string} options.cacheLocation the location of the cache data
	 * @param {string} options.version version identifier
	 * @param {Logger} options.logger a logger
	 * @param {Iterable<string>} options.managedPaths paths managed only by package manager
	 * @param {Iterable<string>} options.immutablePaths immutable paths
	 */
	constructor({
		fs,
		context,
		cacheLocation,
		version,
		logger,
		managedPaths,
		immutablePaths
	}) {
		this.fileSerializer = createFileSerializer(fs);
		this.fileSystemInfo = new FileSystemInfo(fs, {
			managedPaths,
			immutablePaths,
			logger: logger.getChildLogger("webpack.FileSystemInfo")
		});
		this.context = context;
		this.cacheLocation = cacheLocation;
		this.version = version;
		this.logger = logger;
		/** @type {Set<string>} */
		this.buildDependencies = new Set();
		/** @type {LazySet<string>} */
		this.newBuildDependencies = new LazySet();
		/** @type {FileSystemInfo.Snapshot} */
		this.resolveBuildDependenciesSnapshot = undefined;
		/** @type {Map<string, string>} */
		this.resolveResults = undefined;
		/** @type {FileSystemInfo.Snapshot} */
		this.buildSnapshot = undefined;
		/** @type {Promise<Pack>} */
		this.packPromise = this._openPack();
	}

	/**
	 * @returns {Promise<Pack>} the pack
	 */
	_openPack() {
		const { logger, cacheLocation, version } = this;
		/** @type {FileSystemInfo.Snapshot} */
		let buildSnapshot;
		/** @type {Set<string>} */
		let buildDependencies;
		/** @type {Set<string>} */
		let newBuildDependencies;
		/** @type {FileSystemInfo.Snapshot} */
		let resolveBuildDependenciesSnapshot;
		/** @type {Map<string, string>} */
		let resolveResults;
		logger.time("restore pack container");
		return this.fileSerializer
			.deserialize(null, {
				filename: `${cacheLocation}/index.pack`,
				extension: ".pack",
				logger
			})
			.catch(err => {
				if (err.code !== "ENOENT") {
					logger.warn(
						`Restoring pack failed from ${cacheLocation}.pack: ${err}`
					);
					logger.debug(err.stack);
				} else {
					logger.debug(`No pack exists at ${cacheLocation}.pack: ${err}`);
				}
				return undefined;
			})
			.then(packContainer => {
				logger.timeEnd("restore pack container");
				if (!packContainer) return undefined;
				if (!(packContainer instanceof PackContainer)) {
					logger.warn(
						`Restored pack from ${cacheLocation}.pack, but contained content is unexpected.`,
						packContainer
					);
					return undefined;
				}
				if (packContainer.version !== version) {
					logger.log(
						`Restored pack from ${cacheLocation}.pack, but version doesn't match.`
					);
					return undefined;
				}
				logger.time("check build dependencies");
				return Promise.all([
					new Promise((resolve, reject) => {
						this.fileSystemInfo.checkSnapshotValid(
							packContainer.buildSnapshot,
							(err, valid) => {
								if (err) {
									logger.log(
										`Restored pack from ${cacheLocation}.pack, but checking snapshot of build dependencies errored: ${err}.`
									);
									logger.debug(err.stack);
									return resolve(false);
								}
								if (!valid) {
									logger.log(
										`Restored pack from ${cacheLocation}.pack, but build dependencies have changed.`
									);
									return resolve(false);
								}
								buildSnapshot = packContainer.buildSnapshot;
								return resolve(true);
							}
						);
					}),
					new Promise((resolve, reject) => {
						this.fileSystemInfo.checkSnapshotValid(
							packContainer.resolveBuildDependenciesSnapshot,
							(err, valid) => {
								if (err) {
									logger.log(
										`Restored pack from ${cacheLocation}.pack, but checking snapshot of resolving of build dependencies errored: ${err}.`
									);
									logger.debug(err.stack);
									return resolve(false);
								}
								if (valid) {
									resolveBuildDependenciesSnapshot =
										packContainer.resolveBuildDependenciesSnapshot;
									buildDependencies = packContainer.buildDependencies;
									resolveResults = packContainer.resolveResults;
									return resolve(true);
								}
								logger.debug(
									"resolving of build dependencies is invalid, will re-resolve build dependencies"
								);
								this.fileSystemInfo.checkResolveResultsValid(
									packContainer.resolveResults,
									(err, valid) => {
										if (err) {
											logger.log(
												`Restored pack from ${cacheLocation}.pack, but resolving of build dependencies errored: ${err}.`
											);
											logger.debug(err.stack);
											return resolve(false);
										}
										if (valid) {
											newBuildDependencies = packContainer.buildDependencies;
											resolveResults = packContainer.resolveResults;
											return resolve(true);
										}
										logger.log(
											`Restored pack from ${cacheLocation}.pack, but build dependencies resolve to different locations.`
										);
										return resolve(false);
									}
								);
							}
						);
					})
				])
					.catch(err => {
						logger.timeEnd("check build dependencies");
						throw err;
					})
					.then(([buildSnapshotValid, resolveValid]) => {
						logger.timeEnd("check build dependencies");
						if (buildSnapshotValid && resolveValid) {
							logger.time("restore pack");
							const d = packContainer.data();
							logger.timeEnd("restore pack");
							return d;
						}
						return undefined;
					});
			})
			.then(pack => {
				if (pack) {
					this.buildSnapshot = buildSnapshot;
					if (buildDependencies) this.buildDependencies = buildDependencies;
					if (newBuildDependencies)
						this.newBuildDependencies.addAll(newBuildDependencies);
					this.resolveResults = resolveResults;
					this.resolveBuildDependenciesSnapshot = resolveBuildDependenciesSnapshot;
					return pack;
				}
				return new Pack(logger);
			})
			.catch(err => {
				this.logger.warn(
					`Restoring pack from ${cacheLocation}.pack failed: ${err}`
				);
				this.logger.debug(err.stack);
				return new Pack(logger);
			});
	}

	/**
	 * @param {string} identifier unique name for the resource
	 * @param {Etag | null} etag etag of the resource
	 * @param {any} data cached content
	 * @returns {Promise<void>} promise
	 */
	store(identifier, etag, data) {
		return this.packPromise.then(pack => {
			this.logger.debug(`Cached ${identifier} to pack.`);
			pack.set(identifier, etag === null ? null : etag.toString(), data);
		});
	}

	/**
	 * @param {string} identifier unique name for the resource
	 * @param {Etag | null} etag etag of the resource
	 * @returns {Promise<any>} promise to the cached content
	 */
	restore(identifier, etag) {
		return this.packPromise
			.then(pack =>
				pack.get(identifier, etag === null ? null : etag.toString())
			)
			.catch(err => {
				if (err && err.code !== "ENOENT") {
					this.logger.warn(
						`Restoring failed for ${identifier} from pack: ${err}`
					);
					this.logger.debug(err.stack);
				}
			});
	}

	storeBuildDependencies(dependencies) {
		this.newBuildDependencies.addAll(dependencies);
	}

	afterAllStored() {
		return this.packPromise
			.then(pack => {
				if (!pack.invalid) return;
				this.logger.log(`Storing pack...`);
				let promise;
				const newBuildDependencies = new Set();
				for (const dep of this.newBuildDependencies) {
					if (!this.buildDependencies.has(dep)) {
						newBuildDependencies.add(dep);
						this.buildDependencies.add(dep);
					}
				}
				this.newBuildDependencies.clear();
				if (newBuildDependencies.size > 0 || !this.buildSnapshot) {
					this.logger.debug(
						`Capturing build dependencies... (${Array.from(
							newBuildDependencies
						).join(", ")})`
					);
					promise = new Promise((resolve, reject) => {
						this.logger.time("resolve build dependencies");
						this.fileSystemInfo.resolveBuildDependencies(
							this.context,
							newBuildDependencies,
							(err, result) => {
								this.logger.timeEnd("resolve build dependencies");
								if (err) return reject(err);

								this.logger.time("snapshot build dependencies");
								const {
									files,
									directories,
									missing,
									resolveResults,
									resolveDependencies
								} = result;
								if (this.resolveResults) {
									for (const [key, value] of resolveResults) {
										this.resolveResults.set(key, value);
									}
								} else {
									this.resolveResults = resolveResults;
								}
								this.fileSystemInfo.createSnapshot(
									undefined,
									resolveDependencies.files,
									resolveDependencies.directories,
									resolveDependencies.missing,
									{},
									(err, snapshot) => {
										if (err) {
											this.logger.timeEnd("snapshot build dependencies");
											return reject(err);
										}
										if (this.resolveBuildDependenciesSnapshot) {
											this.resolveBuildDependenciesSnapshot = this.fileSystemInfo.mergeSnapshots(
												this.resolveBuildDependenciesSnapshot,
												snapshot
											);
										} else {
											this.resolveBuildDependenciesSnapshot = snapshot;
										}
										this.fileSystemInfo.createSnapshot(
											undefined,
											files,
											directories,
											missing,
											{ hash: true },
											(err, snapshot) => {
												this.logger.timeEnd("snapshot build dependencies");
												if (err) return reject(err);
												this.logger.debug("Captured build dependencies");

												if (this.buildSnapshot) {
													this.buildSnapshot = this.fileSystemInfo.mergeSnapshots(
														this.buildSnapshot,
														snapshot
													);
												} else {
													this.buildSnapshot = snapshot;
												}

												resolve();
											}
										);
									}
								);
							}
						);
					});
				} else {
					promise = Promise.resolve();
				}
				return promise.then(() => {
					this.logger.time(`store pack`);
					//pack.collectGarbage();
					const content = new PackContainer(
						pack,
						this.version,
						this.buildSnapshot,
						this.buildDependencies,
						this.resolveResults,
						this.resolveBuildDependenciesSnapshot
					);
					// You might think this breaks all access to the existing pack
					// which are still referenced, but serializing the pack memorizes
					// all data in the pack and makes it no longer need the backing file
					// So it's safe to replace the pack file
					return this.fileSerializer
						.serialize(content, {
							filename: `${this.cacheLocation}/index.pack`,
							extension: ".pack",
							logger: this.logger
						})
						.then(() => {
							this.logger.timeEnd(`store pack`);
							this.logger.log(`Stored pack`);
						})
						.catch(err => {
							this.logger.timeEnd(`store pack`);
							this.logger.warn(`Caching failed for pack: ${err}`);
							this.logger.debug(err.stack);
						});
				});
			})
			.catch(err => {
				this.logger.warn(`Caching failed for pack: ${err}`);
				this.logger.debug(err.stack);
			});
	}
}

module.exports = PackFileCacheStrategy;
