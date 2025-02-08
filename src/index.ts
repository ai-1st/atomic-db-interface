import { Readable } from 'stream'

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
 * @template T The type of data stored in the item
 */
export interface AtomicDbItem<T>
  extends AtomicDbItemKey {
  /** The actual data stored in the item */
  data?: T

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
export interface AtomicDbInterface<T> {
  /**
   * Get a single item by its key
   * @template T The type of data stored in the item
   * @param key The database item key
   * @returns The found item or undefined if not found
   */
  get(
    key: AtomicDbItemKey
  ): Promise<AtomicDbItem<T> | undefined>

  /**
   * Get multiple items by their keys
   * @template T The type of data stored in the item
   * @param keys The database item keys
   * @returns Array with same length as input keys array. Each element will be the corresponding item or undefined if not found.
   */
  getMany(
    keys: AtomicDbItemKey[]
  ): Promise<(AtomicDbItem<T> | undefined)[]>

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
    items: AtomicDbItem<T>[] | AtomicDbItem<T>
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
    items: AtomicDbItem<T>[] | AtomicDbItem<T>,
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
  ): Promise<AtomicDbItem<T>[]>

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
export class AtomicMemoryDb<T> implements AtomicDbInterface<T> {
  private items: Map<string, AtomicDbItem<T>>
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

  private cleanExpired(map: Map<string, { ttl?: number }>, key: string) {
    const item = map.get(key)
    if (item && this.isExpired(item.ttl)) {
      map.delete(key)
      return true
    }
    return false
  }

  async get(key: AtomicDbItemKey): Promise<AtomicDbItem<T> | undefined> {
    const k = this.getKey(key)
    if (this.cleanExpired(this.items, k)) return undefined
    return this.items.get(k)
  }

  async getMany(keys: AtomicDbItemKey[]): Promise<(AtomicDbItem<T> | undefined)[]> {
    return Promise.all(keys.map(key => this.get(key)))
  }

  async getLock(key: AtomicDbItemKey): Promise<AtomicDbItemLock> {
    const k = this.getKey(key)
    const now = Math.floor(Date.now() / 1000)
    const ttl = now + 24 * 60 * 60 // 24 hours
    
    const existingLock = this.locks.get(k)
    if (existingLock) {
      // If TTL is less than 1h away, recreate with new version
      if (!existingLock.ttl || existingLock.ttl - now < 60 * 60) {
        const newLock: AtomicDbItemLock = {
          pk: key.pk,
          sk: key.sk,
          version: crypto.randomUUID(),
          ttl
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
      version: crypto.randomUUID(),
      ttl
    }
    this.locks.set(k, newLock)
    return newLock
  }

  async set(items: AtomicDbItem<T>[] | AtomicDbItem<T>): Promise<void> {
    const itemArray = Array.isArray(items) ? items : [items]
    for (const item of itemArray) {
      this.items.set(this.getKey(item), item)
    }
  }

  async setAtomic(
    items: AtomicDbItem<T>[] | AtomicDbItem<T>,
    locks: AtomicDbItemLock[] | AtomicDbItemLock
  ): Promise<void> {
    const itemArray = Array.isArray(items) ? items : [items]
    const lockArray = Array.isArray(locks) ? locks : [locks]

    if (itemArray.length !== lockArray.length) {
      throw new Error('Items and locks arrays must have the same length')
    }

    // Check all locks first
    for (let i = 0; i < lockArray.length; i++) {
      const lock = lockArray[i]
      const k = this.getKey(lock)
      const existingLock = this.locks.get(k)
      
      if (!existingLock || existingLock.version !== lock.version) {
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
        version: crypto.randomUUID(),
        ttl: lock.ttl
      }
      this.locks.set(this.getKey(lock), newLock)
    }
  }

  async delete(keys: AtomicDbItemKey[] | AtomicDbItemKey): Promise<void> {
    const keyArray = Array.isArray(keys) ? keys : [keys]
    for (const key of keyArray) {
      const k = this.getKey(key)
      this.items.delete(k)
      this.locks.delete(k)
    }
  }

  async query(query: AtomicDbQuery): Promise<AtomicDbItem<T>[]> {
    const results: AtomicDbItem<T>[] = []
    
    for (const [key, item] of this.items) {
      if (this.cleanExpired(this.items, key)) continue
      
      const [pk, sk] = key.split(':')
      if (pk === query.pk && (!query.sk || sk.startsWith(query.sk))) {
        results.push(item)
      }
    }

    // Sort by sort key
    results.sort((a, b) => (query.reverse ? -1 : 1) * a.sk.localeCompare(b.sk))

    // Apply limit if specified
    return query.limit ? results.slice(0, query.limit) : results
  }

  stream(query: AtomicDbQuery): NodeJS.ReadableStream {
    return Readable.from(this.query(query))
  }
}
