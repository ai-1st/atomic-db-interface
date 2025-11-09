import { Readable } from 'stream'
import { monotonicFactory } from 'ulid'

// Create a single ULID factory instance to ensure monotonic ordering
const ulid = monotonicFactory()

/**
 * Helper function to transform a key for lock storage
 * Automatically prepends "__LOCK__" to the primary key
 */
function getLockKey(
  key: AtomicDbItemKey
): AtomicDbItemKey {
  return {
    pk: `__LOCK__${key.pk}`,
    sk: key.sk,
  }
}

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
 * Queue item with enqueued timestamp and processing state
 */
export interface AtomicDbQueueItem {
  /** Primary key (queue identifier) */
  pk: string
  /** Sort key (item identifier for deduplication) */
  sk: string
  /** The actual data stored in the queue item */
  data: any
  /** ULID of the item when it was enqueued, could be two values if the item
   * was enqueued while being processed.
   */
  enqueued: string[]
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
  /** Sort key (item identifier for deduplication) - required for deduplication */
  sk: string
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
   * @returns Array with same length as input keys array. Each element will be the
   * corresponding item or undefined if not found.
   */
  getMany(
    keys: AtomicDbItemKey[]
  ): Promise<(AtomicDbItem | undefined)[]>

  /**
   * Get a lock object by its key directly from the DB
   * If the item doesn't exist, creates a new one with a version and 24h TTL.
   * If the item exists but TTL is less than 1h away, recreates it with a new version
   * and 24h TTL.
   * Lock objects are separate from regular items and are used for optimistic locking.
   *
   * Optimistic locking pattern:
   * 1. Get lock(s) for the item(s) using getLock
   * 2. Get the current data item(s)
   * 3. Perform business logic calculations
   * 4. Use setAtomic with the lock(s) to update the item(s) atomically
   * 5. If setAtomic throws RaceCondition, the lock version changed - reload locks and retry
   * from step 1
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
   * Lock objects are stored separately from the items and are updated with new versions
   * after successful operations.
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
   * @param items The items to push to the queue (isProcessing and processingTimeout
   * are set automatically)
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
 * Helper functions for queue collection keys
 */
function getFifoKey(
  pk: string,
  ulid: string
): AtomicDbItemKey {
  return {
    pk: `__FIFO__${pk}`,
    sk: ulid,
  }
}

function getDedupKey(
  pk: string,
  sk: string
): AtomicDbItemKey {
  return {
    pk: `__FIFO_DEDUP__${pk}`,
    sk: sk,
  }
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

      for (const item of itemArray) {
        const dedupKey = getDedupKey(
          item.pk,
          item.sk
        )
        const now = Math.floor(Date.now() / 1000)

        // Get lock for dedup record to avoid race conditions
        const dedupLock = await db.getLock(
          dedupKey
        )

        // Read current dedup record
        const dedupRecord = await db.get(dedupKey)
        const dedupData = dedupRecord as
          | (AtomicDbItem & {
              enqueued: string[]
              isProcessing: boolean
              processingTimeout: number
            })
          | undefined

        if (!dedupData) {
          // Not enqueued - create new records
          const newUlid = ulid()
          const fifoKey = getFifoKey(
            item.pk,
            newUlid
          )
          const fifoLock = await db.getLock(
            fifoKey
          )

          // Create FIFO record: { itemKey: <sk>, data: <data> }
          const fifoItem: AtomicDbItem = {
            pk: fifoKey.pk,
            sk: fifoKey.sk,
            data: {
              itemKey: item.sk,
              data: item.data,
            },
          }

          // Create DEDUP record: { enqueued: [ulid], isProcessing: false, processingTimeout: 0 }
          const newDedupItem: AtomicDbItem = {
            pk: dedupKey.pk,
            sk: dedupKey.sk,
            data: {
              enqueued: [newUlid],
              isProcessing: false,
              processingTimeout: 0,
            },
          }

          await db.setAtomic(
            [fifoItem, newDedupItem],
            [fifoLock, dedupLock]
          )
        } else {
          const enqueued =
            dedupData.data?.enqueued || []
          const isProcessing =
            dedupData.data?.isProcessing || false
          const processingTimeout =
            dedupData.data?.processingTimeout || 0
          const isProcessingExpired =
            isProcessing &&
            processingTimeout <= now

          if (
            !isProcessing ||
            isProcessingExpired
          ) {
            // Enqueued but not processing (or expired) - update existing FIFO record
            const oldestUlid = enqueued[0]
            if (!oldestUlid) {
              // Should not happen, but handle gracefully
              const newUlid = ulid()
              const fifoKey = getFifoKey(
                item.pk,
                newUlid
              )
              const fifoLock = await db.getLock(
                fifoKey
              )

              const fifoItem: AtomicDbItem = {
                pk: fifoKey.pk,
                sk: fifoKey.sk,
                data: {
                  itemKey: item.sk,
                  data: item.data,
                },
              }

              const updatedDedupItem: AtomicDbItem =
                {
                  pk: dedupKey.pk,
                  sk: dedupKey.sk,
                  data: {
                    enqueued: [newUlid],
                    isProcessing: false,
                    processingTimeout: 0,
                  },
                }

              await db.setAtomic(
                [fifoItem, updatedDedupItem],
                [fifoLock, dedupLock]
              )
            } else {
              // Update existing FIFO record (maintains position)
              const fifoKey = getFifoKey(
                item.pk,
                oldestUlid
              )
              const fifoLock = await db.getLock(
                fifoKey
              )

              const fifoItem: AtomicDbItem = {
                pk: fifoKey.pk,
                sk: fifoKey.sk,
                data: {
                  itemKey: item.sk,
                  data: item.data,
                },
              }

              const updatedDedupItem: AtomicDbItem =
                {
                  pk: dedupKey.pk,
                  sk: dedupKey.sk,
                  data: {
                    enqueued: enqueued, // Keep same ULID
                    isProcessing: false,
                    processingTimeout: 0,
                  },
                }

              await db.setAtomic(
                [fifoItem, updatedDedupItem],
                [fifoLock, dedupLock]
              )
            }
          } else {
            // Enqueued and currently processing - create new FIFO record
            const newUlid = ulid()
            const fifoKey = getFifoKey(
              item.pk,
              newUlid
            )
            const fifoLock = await db.getLock(
              fifoKey
            )

            const fifoItem: AtomicDbItem = {
              pk: fifoKey.pk,
              sk: fifoKey.sk,
              data: {
                itemKey: item.sk,
                data: item.data,
              },
            }

            // Append new ULID to enqueued array
            const updatedDedupItem: AtomicDbItem =
              {
                pk: dedupKey.pk,
                sk: dedupKey.sk,
                data: {
                  enqueued: [
                    ...enqueued,
                    newUlid,
                  ],
                  isProcessing: isProcessing,
                  processingTimeout:
                    processingTimeout,
                },
              }

            await db.setAtomic(
              [fifoItem, updatedDedupItem],
              [fifoLock, dedupLock]
            )
          }
        }
      }
    },

    async queuePull(
      options: AtomicDbQueuePullOptions
    ): Promise<AtomicDbQueuePullResult> {
      const timeoutSeconds =
        options.ttlSeconds || 300 // Default 5 minutes
      const now = Math.floor(Date.now() / 1000)

      // Query __FIFO__ collection for first item (sorted by ULID)
      const fifoPk = `__FIFO__${options.pk}`
      const items = await db.query({
        pk: fifoPk,
        limit: 1,
      })

      // If no items, return empty
      if (items.length === 0) {
        return {}
      }

      // Get the first (and only) item from FIFO collection
      const fifoItem = items[0]
      const fifoData = fifoItem.data as
        | { itemKey: string; data: any }
        | undefined

      if (!fifoData || !fifoData.itemKey) {
        // Invalid FIFO record, return empty
        return {}
      }

      const itemSk = fifoData.itemKey
      const itemData = fifoData.data

      // Look up dedup record using the itemKey (sk)
      const dedupKey = getDedupKey(
        options.pk,
        itemSk
      )

      try {
        // Get lock for dedup record
        const dedupLock = await db.getLock(
          dedupKey
        )

        // Re-read dedup record to get latest state
        const dedupRecord = await db.get(dedupKey)
        if (!dedupRecord) {
          // Dedup record missing, return empty
          return {}
        }

        const dedupData = dedupRecord.data as
          | {
              enqueued: string[]
              isProcessing: boolean
              processingTimeout: number
            }
          | undefined

        if (!dedupData) {
          return {}
        }

        const enqueued = dedupData.enqueued || []
        const isProcessing =
          dedupData.isProcessing || false
        const processingTimeout =
          dedupData.processingTimeout || 0

        // Check if item is available (not processing or timeout expired)
        const currentNow = Math.floor(
          Date.now() / 1000
        )
        if (
          isProcessing &&
          processingTimeout > currentNow
        ) {
          // Item is still being processed, return empty
          return {}
        }

        // Calculate new processing timeout
        const newProcessingTimeout =
          currentNow + timeoutSeconds

        // Update dedup record to mark as processing
        const updatedDedupItem: AtomicDbItem = {
          pk: dedupKey.pk,
          sk: dedupKey.sk,
          data: {
            enqueued: enqueued,
            isProcessing: true,
            processingTimeout:
              newProcessingTimeout,
          },
        }

        // Use setAtomic to atomically update the dedup record
        await db.setAtomic(
          updatedDedupItem,
          dedupLock
        )

        // Return reconstructed item
        const resultItem: AtomicDbQueueItem = {
          pk: options.pk,
          sk: itemSk,
          data: itemData,
          enqueued: enqueued,
          isProcessing: true,
          processingTimeout: newProcessingTimeout,
        }

        return { item: resultItem }
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
      const dedupKey = getDedupKey(key.pk, key.sk)

      // Get lock for dedup record
      const dedupLock = await db.getLock(dedupKey)

      // Read dedup record to get enqueued array
      const dedupRecord = await db.get(dedupKey)
      if (!dedupRecord) {
        throw new Error('Item not found in queue')
      }

      const dedupData = dedupRecord.data as
        | {
            enqueued: string[]
            isProcessing: boolean
            processingTimeout: number
          }
        | undefined

      if (
        !dedupData ||
        !dedupData.enqueued ||
        dedupData.enqueued.length === 0
      ) {
        throw new Error('Item not found in queue')
      }

      const enqueued = dedupData.enqueued
      const oldestUlid = enqueued[0]

      // Delete FIFO record for oldest ULID
      const fifoKey = getFifoKey(
        key.pk,
        oldestUlid
      )
      await db.delete(fifoKey)

      // Remove first ULID from enqueued array
      const remainingUlids = enqueued.slice(1)

      if (remainingUlids.length === 0) {
        // Delete dedup record if no more ULIDs
        await db.delete(dedupKey)
      } else {
        // Update dedup record with remaining ULIDs
        const updatedDedupItem: AtomicDbItem = {
          pk: dedupKey.pk,
          sk: dedupKey.sk,
          data: {
            enqueued: remainingUlids,
            isProcessing: false,
            processingTimeout: 0,
          },
        }

        await db.setAtomic(
          updatedDedupItem,
          dedupLock
        )
      }
    },

    async queueRelease(
      key: AtomicDbItemKey
    ): Promise<void> {
      const dedupKey = getDedupKey(key.pk, key.sk)

      // Get lock for dedup record
      const dedupLock = await db.getLock(dedupKey)

      // Read dedup record
      const dedupRecord = await db.get(dedupKey)
      if (!dedupRecord) {
        throw new Error('Item not found in queue')
      }

      const dedupData = dedupRecord.data as
        | {
            enqueued: string[]
            isProcessing: boolean
            processingTimeout: number
          }
        | undefined

      if (!dedupData) {
        throw new Error('Item not found in queue')
      }

      const enqueued = dedupData.enqueued || []

      // Update dedup record to mark as not processing
      // Keep enqueued array unchanged
      const updatedDedupItem: AtomicDbItem = {
        pk: dedupKey.pk,
        sk: dedupKey.sk,
        data: {
          enqueued: enqueued,
          isProcessing: false,
          processingTimeout: 0,
        },
      }

      // Use setAtomic to atomically update the dedup record
      try {
        await db.setAtomic(
          updatedDedupItem,
          dedupLock
        )
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
    // Transform key for internal storage (prepend __LOCK__ to pk)
    const lockKey = getLockKey(key)
    const k = this.getKey(lockKey)
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
          pk: key.pk, // Return original key (without __LOCK__ prefix)
          sk: key.sk,
          version: ulid(),
          ttl,
        }
        this.locks.set(k, newLock)
        return newLock
      }
      // Return lock with original key (without __LOCK__ prefix)
      return {
        ...existingLock,
        pk: key.pk,
        sk: key.sk,
      }
    }

    // Create new lock
    const newLock: AtomicDbItemLock = {
      pk: key.pk, // Return original key (without __LOCK__ prefix)
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
      // Transform lock key for internal lookup (prepend __LOCK__ to pk)
      const lockKey = getLockKey(lock)
      const k = this.getKey(lockKey)
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
      const itemK = this.getKey(item)

      // Update item
      this.items.set(itemK, item)

      // Update lock with new version (using transformed key for storage)
      const lockKey = getLockKey(lock)
      const lockK = this.getKey(lockKey)
      const newLock: AtomicDbItemLock = {
        pk: lock.pk, // Store original key in lock object
        sk: lock.sk,
        version: monotonicFactory()(),
        ttl: lock.ttl,
      }
      this.locks.set(lockK, newLock)
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
      // Delete lock using transformed key (prepend __LOCK__ to pk)
      const lockKey = getLockKey(key)
      const lockK = this.getKey(lockKey)
      this.locks.delete(lockK)
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
