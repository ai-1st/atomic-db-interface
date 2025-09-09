# Atomic DB Interface

A TypeScript interface and implementations for databases supporting atomic operations with optimistic locking.

## Features

- **Database Agnostic Interface**: Provides a common interface for any database implementation
- **Atomic Operations**: Perform atomic updates with optimistic locking to prevent race conditions
- **Separate Lock Objects**: Lock objects are stored separately from items, allowing for flexible locking strategies
- **Automatic Lock Management**: Locks automatically expire after 24 hours and refresh when nearing expiration
- **Type Safety**: Full TypeScript support with generic types for item data
- **Streaming**: Stream query results for efficient processing of large datasets
- **Batch Operations**: Efficient batch operations for non-atomic updates
- **In-Memory Implementation**: Includes a ready-to-use in-memory implementation for testing
- **LRU Caching**: Optional LRU cache wrapper for improved performance

## Installation

```bash
npm install atomic-db-interface
```

## Usage

### Basic Setup with In-Memory Implementation

```typescript
import { AtomicMemoryDb } from 'atomic-db-interface'

const db = new AtomicMemoryDb()
```

### Setup with LRU Cache

```typescript
import {
  AtomicMemoryDb,
  AtomicLRUCache,
} from 'atomic-db-interface'

const memDb = new AtomicMemoryDb()
const db = new AtomicLRUCache(memDb, 1000) // Cache size of 1000 items
```

### Simple Operations

```typescript
// Set an item
await db.set({
  pk: 'user#123',
  sk: 'profile',
  data: { name: 'John', age: 30 },
})

// Get an item
const item = await db.get({
  pk: 'user#123',
  sk: 'profile',
})

// Delete an item
await db.delete({
  pk: 'user#123',
  sk: 'profile',
})
```

### Atomic Operations

```typescript
import { RaceCondition } from 'atomic-db-interface'

// Define keys for the data item and its lock
const itemKey = {
  pk: 'user#123',
  sk: 'counter',
}
const lockKey = {
  pk: 'user#123',
  sk: 'counter#lock',
}

// Get or create a lock (automatically expires after 24 hours)
const lock = await db.getLock(lockKey)

// Update item atomically
try {
  await db.setAtomic(
    {
      pk: itemKey.pk,
      sk: itemKey.sk,
      data: { value: 42 },
    },
    lock
  )
} catch (e) {
  if (e instanceof RaceCondition) {
    // Handle concurrent modification
  }
  throw e
}

// Clean up (optional)
await db.delete([lockKey, itemKey])
```

### Best Practices for Locks

1. **Separate Keys**: Always use different keys for locks and data items

   ```typescript
   // Good
   const itemKey = { pk: 'user#123', sk: 'data' }
   const lockKey = {
     pk: 'user#123',
     sk: 'data#lock',
   }

   // Bad - using same key for both
   const key = { pk: 'user#123', sk: 'data' }
   ```

2. **Consistent Naming**: Use a predictable pattern for lock keys

   ```typescript
   // Examples:
   sk: 'profile#lock' // For profile data
   sk: 'settings#lock' // For settings data
   sk: 'counter#lock' // For counter data
   ```

3. **Lock Lifecycle**: Locks are automatically managed

   - New locks expire after 24 hours
   - Locks are automatically refreshed when accessed within their last hour
   - No manual TTL management required

4. **Clean Up**: Remember to delete locks when they're no longer needed
   ```typescript
   // Clean up both the data and lock
   await db.delete([itemKey, lockKey])
   ```

### Batch Operations

```typescript
// Set multiple items
await db.set([
  {
    pk: 'user#123',
    sk: 'profile',
    data: { name: 'John' },
  },
  {
    pk: 'user#123',
    sk: 'settings',
    data: { theme: 'dark' },
  },
])

// Get multiple items
const items = await db.getMany([
  { pk: 'user#123', sk: 'profile' },
  { pk: 'user#123', sk: 'settings' },
])
```

### Query Operations

```typescript
// Query by partition key
const results = await db.query({
  pk: 'user#123',
})

// Query with sort key prefix
const results = await db.query({
  pk: 'user#123',
  sk: 'profile#',
})

// Stream results
const stream = db.stream({
  pk: 'user#123',
})
```

## Interface and Implementations

This package provides the `AtomicDbInterface` which can be implemented by any database adapter. It includes two ready-to-use implementations:

### AtomicMemoryDb

An in-memory implementation perfect for testing and development. Supports all features including TTL-based expiration.

### AtomicLRUCache

A wrapper that adds LRU caching to any `AtomicDbInterface` implementation. Does not cache locks (they must always be fresh) or query results.

### Custom Implementations

You can implement `AtomicDbInterface` for any database. The interface defines:

- `get(key)` - Get a single item
- `getMany(keys)` - Get multiple items
- `getLock(key)` - Get or create a lock with automatic TTL management
- `set(items)` - Set items without atomicity
- `setAtomic(items, locks)` - Set items atomically with lock verification
- `delete(keys)` - Delete items
- `query(query)` - Query items by primary key and optional sort key prefix
- `stream(query)` - Stream query results

## Lock Management

The interface uses optimistic locking with automatic TTL management to prevent race conditions in atomic operations. Here's how it works:

1. Lock objects are stored separately from the actual items using different sort keys
2. Each lock object has a version that's updated on every atomic operation
3. Locks automatically expire after 24 hours via TTL feature
4. When a lock is accessed within its last hour of validity, it's automatically refreshed with a new 24-hour TTL
5. The `setAtomic` method requires both the item to update and its corresponding lock
6. If the lock's version has changed since it was read, the operation fails with a `RaceCondition` error

This approach allows for:

- Atomic updates across multiple items
- Clear separation between data and lock storage
- Automatic cleanup of stale locks via TTL
- Zero-maintenance lock management

## Error Handling

The interface defines the following error types:

- `RaceCondition`: Thrown when an atomic operation fails due to concurrent modifications
- `Error`: Standard error for invalid operations or database-specific errors

## TypeScript Types

The package exports these key types:

- `AtomicDbInterface`: Main interface for database implementations
- `AtomicDbItemKey`: Database item key structure (pk, sk)
- `AtomicDbItem`: Generic database item with optional data and TTL
- `AtomicDbItemLock`: Lock object with version and TTL
- `AtomicDbQuery`: Query options for database operations
- `RaceCondition`: Error class for race condition detection

## License

MIT
