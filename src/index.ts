import { Readable } from 'stream'
import { monotonicFactory } from 'ulid'

// Error types for database operations
export class RaceCondition extends Error {
  constructor() {
    super('Race condition detected')
    this.name = 'RaceCondition'
  }
}

/**
 * Database item key structure
 */
export interface AtomicDbItemKey {
  /** Primary key */
  pk: string
  /** Sort key */
  sk: string
}

/**
 * Generic database item with TTL
 */
export interface AtomicDbItem
  extends AtomicDbItemKey {
  /** The actual data stored in the item */
  data?: any

  /**
   * Epoch time in seconds after which the item will be deleted by the database
   */
  ttl?: number
}

/**
 * Lock object for database items
 */
export interface AtomicDbItemLock
  extends AtomicDbItemKey {
  version: string
  ttl?: number
}

/**
 * Query options for database operations
 */
export interface AtomicDbQuery {
  /** Primary key to query */
  pk: string
  /** Optional sort key prefix */
  sk?: string
  /** If true, returns results in reverse order */
  reverse?: boolean
  /** Maximum number of items to return */
  limit?: number
}

/**
 * Base interface for database operations
 */
export interface AtomicDbInterface {
  /**
   * Get a single item by its key
   * @param key The database item key
   * @returns The found item or undefined if not found
   */
  get(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItem | undefined>

  /**
   * Get multiple items by their keys
   * @param keys The database item keys
   * @returns Array with same length as input keys array. Each element will be the corresponding item or undefined if not found.
   */
  getMany(
    keys: AtomicDbItemKey[]
  ): Promise<(AtomicDbItem | undefined)[]>

  /**
   * Get a lock object by its key directly from the DB
   * If the item doesn't exist, creates a new one with a version and 24h TTL.
   * If the item exists but TTL is less than 1h away, recreates it with a new version and 24h TTL.
   * Lock objects are separate from regular items and are used for optimistic locking.
   * @param key The database item key
   * @returns The found lock object or a new one with initial version
   */
  getLock(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItemLock>

  /**
   * Set one or multiple items using fast and cost-efficient BatchWriteItem command
   * This operation does not perform any version checks.
   * @param items The items to set
   */
  set(
    items: AtomicDbItem[] | AtomicDbItem
  ): Promise<void>

  /**
   * Set one or multiple items atomically with optimistic locking
   * Each item requires a corresponding lock object for version checking.
   * Lock objects are stored separately from the items and are updated with new versions after successful operations.
   * @param items The items to set
   * @param locks The lock objects to check versions against. Must match items one-to-one.
   * @throws {RaceCondition} If version check fails
   */
  setAtomic(
    items: AtomicDbItem[] | AtomicDbItem,
    locks: AtomicDbItemLock[] | AtomicDbItemLock
  ): Promise<void>

  /**
   * Delete one or multiple items
   * @param keys The keys of items to delete
   */
  delete(
    keys: AtomicDbItemKey[] | AtomicDbItemKey
  ): Promise<void>

  /**
   * Query items by primary key and optional sort key prefix
   * @param query The query parameters
   * @returns Array of matching items
   */
  query(
    query: AtomicDbQuery
  ): Promise<AtomicDbItem[]>

  /**
   * Stream items by primary key and optional sort key prefix
   * @param query The query parameters
   * @returns Readable stream of matching items
   */
  stream(
    query: AtomicDbQuery
  ): NodeJS.ReadableStream
}

/**
 * In-memory implementation of AtomicDbInterface for testing purposes
 */
export class AtomicMemoryDb
  implements AtomicDbInterface
{
  private items: Map<string, AtomicDbItem>
  private locks: Map<string, AtomicDbItemLock>

  constructor() {
    this.items = new Map()
    this.locks = new Map()
  }

  private getKey(key: AtomicDbItemKey): string {
    return `${key.pk}:${key.sk}`
  }

  private isExpired(ttl?: number): boolean {
    if (!ttl) return false
    return Math.floor(Date.now() / 1000) > ttl
  }

  private cleanExpired(
    map: Map<string, { ttl?: number }>,
    key: string
  ) {
    const item = map.get(key)
    if (item && this.isExpired(item.ttl)) {
      map.delete(key)
      return true
    }
    return false
  }

  async get(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItem | undefined> {
    const k = this.getKey(key)
    if (this.cleanExpired(this.items, k))
      return undefined
    return this.items.get(k)
  }

  async getMany(
    keys: AtomicDbItemKey[]
  ): Promise<(AtomicDbItem | undefined)[]> {
    return Promise.all(
      keys.map((key) => this.get(key))
    )
  }

  async getLock(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItemLock> {
    const k = this.getKey(key)
    const now = Math.floor(Date.now() / 1000)
    const ttl = now + 24 * 60 * 60 // 24 hours

    const existingLock = this.locks.get(k)
    if (existingLock) {
      // If TTL is less than 1h away, recreate with new version
      if (
        !existingLock.ttl ||
        existingLock.ttl - now < 60 * 60
      ) {
        const newLock: AtomicDbItemLock = {
          pk: key.pk,
          sk: key.sk,
          version: monotonicFactory()(),
          ttl,
        }
        this.locks.set(k, newLock)
        return newLock
      }
      return existingLock
    }

    // Create new lock
    const newLock: AtomicDbItemLock = {
      pk: key.pk,
      sk: key.sk,
      version: monotonicFactory()(),
      ttl,
    }
    this.locks.set(k, newLock)
    return newLock
  }

  async set(
    items: AtomicDbItem[] | AtomicDbItem
  ): Promise<void> {
    const itemArray = Array.isArray(items)
      ? items
      : [items]
    for (const item of itemArray) {
      this.items.set(this.getKey(item), item)
    }
  }

  async setAtomic(
    items: AtomicDbItem[] | AtomicDbItem,
    locks: AtomicDbItemLock[] | AtomicDbItemLock
  ): Promise<void> {
    const itemArray = Array.isArray(items)
      ? items
      : [items]
    const lockArray = Array.isArray(locks)
      ? locks
      : [locks]

    if (itemArray.length !== lockArray.length) {
      throw new Error(
        'Items and locks arrays must have the same length'
      )
    }

    // Check all locks first
    for (let i = 0; i < lockArray.length; i++) {
      const lock = lockArray[i]
      const k = this.getKey(lock)
      const existingLock = this.locks.get(k)

      if (
        !existingLock ||
        existingLock.version !== lock.version
      ) {
        throw new RaceCondition()
      }
    }

    // If all locks are valid, update items and locks
    for (let i = 0; i < itemArray.length; i++) {
      const item = itemArray[i]
      const lock = lockArray[i]
      const k = this.getKey(item)

      // Update item
      this.items.set(k, item)

      // Update lock with new version
      const newLock: AtomicDbItemLock = {
        pk: lock.pk,
        sk: lock.sk,
        version: monotonicFactory()(),
        ttl: lock.ttl,
      }
      this.locks.set(this.getKey(lock), newLock)
    }
  }

  async delete(
    keys: AtomicDbItemKey[] | AtomicDbItemKey
  ): Promise<void> {
    const keyArray = Array.isArray(keys)
      ? keys
      : [keys]
    for (const key of keyArray) {
      const k = this.getKey(key)
      this.items.delete(k)
      this.locks.delete(k)
    }
  }

  async query(
    query: AtomicDbQuery
  ): Promise<AtomicDbItem[]> {
    const results: AtomicDbItem[] = []

    for (const [key, item] of this.items) {
      if (this.cleanExpired(this.items, key))
        continue

      const [itemPk, ...skParts] = key.split(':')
      const itemSk = skParts.join(':')

      if (
        itemPk === query.pk &&
        (!query.sk || itemSk.startsWith(query.sk))
      ) {
        results.push(item)
      }
    }

    // Sort by sort key
    results.sort((a, b) => {
      const comparison = a.sk.localeCompare(b.sk)
      return query.reverse
        ? -comparison
        : comparison
    })

    // Apply limit if specified
    return query.limit
      ? results.slice(0, query.limit)
      : results
  }

  stream(
    query: AtomicDbQuery
  ): NodeJS.ReadableStream {
    const self = this
    return new Readable({
      objectMode: true,
      async read() {
        try {
          const results = await self.query(query)
          for (const result of results) {
            this.push(result)
          }
          this.push(null)
        } catch (err) {
          this.destroy(err as Error)
        }
      },
    })
  }
}

/**
 * LRU Cache wrapper for AtomicDbInterface implementations
 */
export class AtomicLRUCache
  implements AtomicDbInterface
{
  private db: AtomicDbInterface
  private cache: any // LRUCacheWithDelete type
  private getKey: (key: AtomicDbItemKey) => string

  constructor(
    db: AtomicDbInterface,
    cacheSize: number
  ) {
    const LRUCacheWithDelete = require('mnemonist/lru-cache-with-delete')
    this.db = db
    this.cache = new LRUCacheWithDelete(cacheSize)
    this.getKey = (key: AtomicDbItemKey) =>
      `${key.pk}/${key.sk}`
  }

  async get(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItem | undefined> {
    const cacheKey = this.getKey(key)
    const cached = this.cache.get(cacheKey)
    if (cached !== undefined) {
      return cached
    }

    const item = await this.db.get(key)
    if (item !== undefined) {
      this.cache.set(cacheKey, item)
    }
    return item
  }

  async getMany(
    keys: AtomicDbItemKey[]
  ): Promise<(AtomicDbItem | undefined)[]> {
    const results: (AtomicDbItem | undefined)[] =
      new Array(keys.length)
    const missingIndices: number[] = []
    const missingKeys: AtomicDbItemKey[] = []

    // Check cache first
    keys.forEach((key, index) => {
      const cacheKey = this.getKey(key)
      const cached = this.cache.get(cacheKey)
      if (cached !== undefined) {
        results[index] = cached
      } else {
        missingIndices.push(index)
        missingKeys.push(key)
      }
    })

    // Fetch missing items from DB
    if (missingKeys.length > 0) {
      const dbResults = await this.db.getMany(
        missingKeys
      )
      dbResults.forEach((item, i) => {
        const index = missingIndices[i]
        results[index] = item
        if (item !== undefined) {
          this.cache.set(
            this.getKey(missingKeys[i]),
            item
          )
        }
      })
    }

    return results
  }

  async getLock(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItemLock> {
    // Don't cache locks as they need to be fresh
    return this.db.getLock(key)
  }

  async set(
    items: AtomicDbItem[] | AtomicDbItem
  ): Promise<void> {
    const itemArray = Array.isArray(items)
      ? items
      : [items]
    // Invalidate cache entries before setting because
    // set operation may partially succeed and throw an error
    itemArray.forEach((item) => {
      this.cache.delete(this.getKey(item))
    })
    await this.db.set(items)
    // Update cache entries after setting
    // Note: the cache may get out of sync if there are concurrent
    // set operations, but that is as designed. If up-to-date data
    // is important, the uncached interface can be used to bypass the cache.
    itemArray.forEach((item) => {
      this.cache.set(this.getKey(item), item)
    })
  }

  async setAtomic(
    items: AtomicDbItem[] | AtomicDbItem,
    locks: AtomicDbItemLock[] | AtomicDbItemLock
  ): Promise<void> {
    const itemArray = Array.isArray(items)
      ? items
      : [items]
    await this.db.setAtomic(items, locks)
    // Update cache entries after setting. Note: local caches are not meant
    // to maintain consistency with the DB in a distributed setup.
    // If up-to-date data is important, the uncached interface can be used
    // to bypass the cache.
    itemArray.forEach((item) => {
      this.cache.set(this.getKey(item), item)
    })
  }

  async delete(
    keys: AtomicDbItemKey[] | AtomicDbItemKey
  ): Promise<void> {
    const keyArray = Array.isArray(keys)
      ? keys
      : [keys]
    // Invalidate cache entries
    keyArray.forEach((key) => {
      this.cache.delete(this.getKey(key))
    })
    await this.db.delete(keys)
  }

  async query(
    query: AtomicDbQuery
  ): Promise<AtomicDbItem[]> {
    // Don't cache query results as they may be partial
    return this.db.query(query)
  }

  stream(
    query: AtomicDbQuery
  ): NodeJS.ReadableStream {
    // Don't cache stream results
    return this.db.stream(query)
  }
}
