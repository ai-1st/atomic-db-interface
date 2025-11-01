import { Readable } from 'stream'
import { monotonicFactory } from 'ulid'

// Create a single ULID factory instance to ensure monotonic ordering
const ulid = monotonicFactory()

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
 * Queue item with optional sk (auto-generated if not provided)
 */
export interface AtomicDbQueueItem
  extends AtomicDbItem {
  /** Whether the item is currently being processed */
  isProcessing: boolean
  /** Epoch time in seconds when processing timeout expires */
  processingTimeout: number
}

/**
 * Input type for queuePush - fields isProcessing and processingTimeout are set automatically
 */
export interface AtomicDbQueueItemInput {
  /** Primary key (queue identifier) */
  pk: string
  /** Sort key - if not provided, ULID will be generated */
  sk?: string
  /** The actual data stored in the queue item */
  data: any
}

/**
 * Options for pulling from queue
 */
export interface AtomicDbQueuePullOptions {
  /** Queue identifier (primary key) */
  pk: string
  /** Visibility timeout in seconds (lock duration) */
  ttlSeconds?: number
}

/**
 * Result of a pull operation
 */
export interface AtomicDbQueuePullResult {
  /** Single item pulled (undefined if queue empty or all locked) */
  item?: AtomicDbQueueItem
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
   *
   * Optimistic locking pattern:
   * 1. Get lock(s) for the item(s) using getLock
   * 2. Get the current data item(s)
   * 3. Perform business logic calculations
   * 4. Use setAtomic with the lock(s) to update the item(s) atomically
   * 5. If setAtomic throws RaceCondition, the lock version changed - reload locks and retry from step 1
   *
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

  /**
   * Push one or more items to a FIFO queue
   * @param items The items to push to the queue (isProcessing and processingTimeout are set automatically)
   */
  queuePush(
    items:
      | AtomicDbQueueItemInput
      | AtomicDbQueueItemInput[]
  ): Promise<void>

  /**
   * Pull one item from queue with locking
   * @param options The pull options including queue identifier and TTL
   * @returns The pulled item, or undefined if no items available
   */
  queuePull(
    options: AtomicDbQueuePullOptions
  ): Promise<AtomicDbQueuePullResult>

  /**
   * Acknowledge and delete one item from queue
   * @param key The key of the item to acknowledge
   */
  queueAcknowledge(
    key: AtomicDbItemKey
  ): Promise<void>

  /**
   * Release an item back to the queue before timeout expires
   * Makes the item available for other consumers immediately
   * @param key The key of the item to release
   */
  queueRelease(
    key: AtomicDbItemKey
  ): Promise<void>
}

/**
 * Helper function to implement queue methods using atomic operations
 * This provides a shared implementation for all AtomicDbInterface implementations
 */
export function createQueueMethods(
  db: AtomicDbInterface
) {
  return {
    async queuePush(
      items:
        | AtomicDbQueueItemInput
        | AtomicDbQueueItemInput[]
    ): Promise<void> {
      const itemArray = Array.isArray(items)
        ? items
        : [items]
      const itemsToSet: AtomicDbQueueItem[] =
        itemArray.map((item) => ({
          pk: item.pk,
          sk: item.sk || ulid(),
          data: item.data,
          isProcessing: false,
          processingTimeout: 0,
        }))
      await db.set(itemsToSet)
    },

    async queuePull(
      options: AtomicDbQueuePullOptions
    ): Promise<AtomicDbQueuePullResult> {
      const timeoutSeconds =
        options.ttlSeconds || 300 // Default 5 minutes
      const now = Math.floor(Date.now() / 1000)

      // Query for the first item in the queue (FIFO - limit=1)
      const items = await db.query({
        pk: options.pk,
        limit: 1,
      })

      // If no items, return empty
      if (items.length === 0) {
        return {}
      }

      // Get the first (and only) item
      const item = items[0]
      const queueItem = item as AtomicDbQueueItem

      // Check if item is processing and hasn't timed out
      const isProcessing =
        queueItem.isProcessing ?? false
      const timeout =
        queueItem.processingTimeout ?? 0
      if (isProcessing && timeout > now) {
        // Item is still being processed, return empty
        return {}
      }

      // Try to acquire this item
      const lockKey = {
        pk: item.pk,
        sk: item.sk,
      }

      try {
        // Get lock first (optimistic locking pattern)
        const lock = await db.getLock(lockKey)

        // Re-read the item to get latest state
        const currentItem = await db.get(lockKey)
        if (!currentItem) {
          // Item was deleted, return empty
          return {}
        }

        const currentQueueItem =
          currentItem as AtomicDbQueueItem

        // Re-check if item is still available (may have been updated by another consumer)
        const currentNow = Math.floor(
          Date.now() / 1000
        )
        const currentIsProcessing =
          currentQueueItem.isProcessing ?? false
        const currentTimeout =
          currentQueueItem.processingTimeout ?? 0
        if (
          currentIsProcessing &&
          currentTimeout > currentNow
        ) {
          // Item is now being processed by another consumer, return empty
          return {}
        }

        // Calculate processing timeout based on current time
        const processingTimeout =
          currentNow + timeoutSeconds

        // Update item to isProcessing=true with new timeout
        const updatedItem: AtomicDbQueueItem = {
          ...currentQueueItem,
          isProcessing: true,
          processingTimeout: processingTimeout,
        }

        // Use setAtomic to atomically update the item
        await db.setAtomic(updatedItem, lock)

        return { item: updatedItem }
      } catch (e) {
        // RaceCondition - lock version changed, return empty
        if (e instanceof RaceCondition) {
          return {}
        }
        throw e
      }
    },

    async queueAcknowledge(
      key: AtomicDbItemKey
    ): Promise<void> {
      await db.delete(key)
    },

    async queueRelease(
      key: AtomicDbItemKey
    ): Promise<void> {
      // Get the current item
      const item = await db.get(key)
      if (!item) {
        throw new Error('Item not found')
      }

      const queueItem = item as AtomicDbQueueItem

      // Get lock for the item
      const lock = await db.getLock(key)

      // Update item to mark as not processing
      const updatedItem: AtomicDbQueueItem = {
        ...queueItem,
        isProcessing: false,
        processingTimeout: 0,
      }

      // Use setAtomic to atomically update the item
      try {
        await db.setAtomic(updatedItem, lock)
      } catch (e) {
        if (e instanceof RaceCondition) {
          // Item was modified concurrently, which is fine
          // The release operation can be retried if needed
          throw e
        }
        throw e
      }
    },
  }
}

/**
 * In-memory implementation of AtomicDbInterface for testing purposes
 */
export class AtomicMemoryDb
  implements AtomicDbInterface
{
  private items: Map<string, AtomicDbItem>
  private locks: Map<string, AtomicDbItemLock>

  // Queue methods
  queuePush: (
    items:
      | AtomicDbQueueItemInput
      | AtomicDbQueueItemInput[]
  ) => Promise<void>
  queuePull: (
    options: AtomicDbQueuePullOptions
  ) => Promise<AtomicDbQueuePullResult>
  queueAcknowledge: (
    key: AtomicDbItemKey
  ) => Promise<void>
  queueRelease: (
    key: AtomicDbItemKey
  ) => Promise<void>

  constructor() {
    this.items = new Map()
    this.locks = new Map()

    // Initialize queue methods
    const queueMethods = createQueueMethods(this)
    this.queuePush = queueMethods.queuePush
    this.queuePull = queueMethods.queuePull
    this.queueAcknowledge =
      queueMethods.queueAcknowledge
    this.queueRelease = queueMethods.queueRelease
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
          version: ulid(),
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

  // Queue methods
  queuePush: (
    items:
      | AtomicDbQueueItemInput
      | AtomicDbQueueItemInput[]
  ) => Promise<void>
  queuePull: (
    options: AtomicDbQueuePullOptions
  ) => Promise<AtomicDbQueuePullResult>
  queueAcknowledge: (
    key: AtomicDbItemKey
  ) => Promise<void>
  queueRelease: (
    key: AtomicDbItemKey
  ) => Promise<void>

  constructor(
    db: AtomicDbInterface,
    cacheSize: number
  ) {
    const LRUCacheWithDelete = require('mnemonist/lru-cache-with-delete')
    this.db = db
    this.cache = new LRUCacheWithDelete(cacheSize)
    this.getKey = (key: AtomicDbItemKey) =>
      `${key.pk}/${key.sk}`

    // Initialize queue methods
    const queueMethods = createQueueMethods(this)
    this.queuePush = queueMethods.queuePush
    this.queuePull = queueMethods.queuePull
    this.queueAcknowledge =
      queueMethods.queueAcknowledge
    this.queueRelease = queueMethods.queueRelease
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
