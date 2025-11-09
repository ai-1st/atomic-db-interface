# Atomic DB Interface

A TypeScript interface and implementations for databases supporting atomic operations with optimistic locking.

## Features

- **Database Agnostic Interface**: Provides a common interface for any database implementation
- **Atomic Operations**: Perform atomic updates with optimistic locking to resolve race conditions
- **Separate Lock Objects**: Lock objects are stored separately from items, allowing for flexible locking strategies
- **Automatic Lock Management**: Locks automatically expire and refresh when nearing expiration
- **FIFO Queue Support**: Built-in FIFO queue operations with optimistic locking and visibility timeout
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

// Define the key for the data item
// getLock and setAtomic automatically handle lock key transformation
const itemKey = {
  pk: 'user#123',
  sk: 'counter',
}

// Get or create a lock (automatically expires after 24 hours)
// getLock automatically prepends "__LOCK__" to the pk internally
const lock = await db.getLock(itemKey)

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
// delete automatically removes both the item and its lock
await db.delete(itemKey)
```

### Best Practices for Locks

1. **Use the Same Key**: You can use the same key for both the item and its lock

   `getLock` and `setAtomic` automatically handle lock key transformation by prepending `"__LOCK__"` to the primary key internally. You never need to manually create separate lock keys.

   ```typescript
   // Good - use the same key for both item and lock
   const itemKey = { pk: 'user#123', sk: 'data' }
   const lock = await db.getLock(itemKey) // Automatically uses __LOCK__user#123 internally
   await db.setAtomic(item, lock) // Works with the same key

   // The library automatically transforms the lock key internally:
   // Item stored at: { pk: 'user#123', sk: 'data' }
   // Lock stored at: { pk: '__LOCK__user#123', sk: 'data' }
   ```

2. **Lock Lifecycle**: Locks are automatically managed

   - New locks expire after 24 hours
   - Locks are automatically refreshed when accessed within their last hour
   - No manual TTL management required

3. **Clean Up**: The `delete` method automatically removes both the item and its lock
   ```typescript
   // Clean up both the data and lock automatically
   await db.delete(itemKey)
   // Internally deletes both:
   // - Item at: { pk: 'user#123', sk: 'data' }
   // - Lock at: { pk: '__LOCK__user#123', sk: 'data' }
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

### FIFO Queue Operations

The interface provides built-in support for FIFO (First-In-First-Out) queues with optimistic locking and visibility timeout. Queue items use `isProcessing` and `processingTimeout` fields to track processing state.

```typescript
// Push items to a queue
await db.queuePush([
  {
    pk: 'jobs',
    data: {
      task: 'process-image',
      imageId: '123',
    },
  },
  {
    pk: 'jobs',
    data: {
      task: 'send-email',
      to: 'user@example.com',
    },
  },
  {
    pk: 'jobs',
    data: {
      task: 'generate-report',
      reportId: '456',
    },
  },
])

// Pull an item from the queue (with 5-minute visibility timeout)
const result = await db.queuePull({
  pk: 'jobs',
  ttlSeconds: 300, // 5 minutes
})

if (result.item) {
  const itemKey = {
    pk: result.item.pk,
    sk: result.item.sk,
  }

  try {
    // Process the item
    const task = result.item.data
    console.log('Processing:', task)

    // ... do work ...

    // Acknowledge completion (deletes item)
    await db.queueAcknowledge(itemKey)
  } catch (error) {
    // If processing fails, you can manually release it immediately
    // or let the timeout expire naturally
    console.error('Processing failed:', error)

    // Option 1: Manually release immediately
    await db.queueRelease(itemKey)

    // Option 2: Let timeout expire (no action needed)
    // The item will become available again after processingTimeout expires
  }
} else {
  console.log(
    'Queue is empty or all items are being processed'
  )
}
```

#### Queue Features

**FIFO Ordering**: Items are pulled in the order they were added (using ULID for automatic ordering)

```typescript
// Auto-generated sort keys ensure FIFO order
await db.queuePush({
  pk: 'queue1',
  data: 'first',
})
await db.queuePush({
  pk: 'queue1',
  data: 'second',
})
await db.queuePush({
  pk: 'queue1',
  data: 'third',
})

// Items will be pulled in order: first, second, third
```

**Custom Sort Keys**: You can also provide custom sort keys for explicit ordering

```typescript
await db.queuePush([
  { pk: 'tasks', sk: 'task-001', data: 'A' },
  { pk: 'tasks', sk: 'task-002', data: 'B' },
  { pk: 'tasks', sk: 'task-003', data: 'C' },
])
```

**Item-Level Processing**: Each pulled item is marked as processing with a timeout, allowing multiple consumers to process different items concurrently

```typescript
// Multiple consumers can pull and process different items simultaneously
const consumer1 = async () => {
  const result = await db.queuePull({
    pk: 'jobs',
  })
  if (result.item) {
    const itemKey = {
      pk: result.item.pk,
      sk: result.item.sk,
    }
    // Process item...
    await db.queueAcknowledge(itemKey)
  }
}

const consumer2 = async () => {
  const result = await db.queuePull({
    pk: 'jobs',
  })
  if (result.item) {
    const itemKey = {
      pk: result.item.pk,
      sk: result.item.sk,
    }
    // Process different item...
    await db.queueAcknowledge(itemKey)
  }
}

// Both consumers can work concurrently
await Promise.all([consumer1(), consumer2()])
```

**Visibility Timeout**: Items being processed automatically become available again if the processing timeout expires

```typescript
// Pull with 30-second timeout
const result = await db.queuePull({
  pk: 'jobs',
  ttlSeconds: 30,
})

// The item is marked as isProcessing=true with processingTimeout set
// If the item is not acknowledged within 30 seconds,
// it becomes available for other consumers to pull
```

**Manual Release**: You can manually release an item back to the queue before the timeout expires

```typescript
const result = await db.queuePull({ pk: 'jobs' })
if (result.item) {
  try {
    // Attempt to process...
    await processItem(result.item)
    await db.queueAcknowledge({
      pk: result.item.pk,
      sk: result.item.sk,
    })
  } catch (error) {
    // Release immediately instead of waiting for timeout
    await db.queueRelease({
      pk: result.item.pk,
      sk: result.item.sk,
    })
  }
}
```

**Empty Queue Handling**

```typescript
const result = await db.queuePull({ pk: 'jobs' })

if (!result.item) {
  console.log(
    'No items available (queue empty or all items being processed)'
  )
}
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
- `queuePush(items)` - Push items to a FIFO queue
- `queuePull(options)` - Pull one item from queue with optimistic locking
- `queueAcknowledge(key)` - Acknowledge and delete item from queue
- `queueRelease(key)` - Release item back to queue before timeout expires

## Lock Management

The interface uses optimistic locking with automatic TTL management to prevent race conditions in atomic operations. Here's how it works:

1. Lock objects are stored separately from the actual items - `getLock` and `setAtomic` automatically prepend `"__LOCK__"` to the primary key internally, so you can use the same key for both items and locks
2. Each lock object has a version that's updated on every atomic operation
3. Locks automatically expire after 24 hours via TTL feature
4. When a lock is accessed within its last hour of validity, it's automatically refreshed with a new 24-hour TTL
5. The `setAtomic` method updates both the item and its corresponding lock in a single transaction
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
- `AtomicDbQueueItem`: Queue item extending AtomicDbItem with isProcessing and processingTimeout fields
- `AtomicDbQueuePullOptions`: Options for pulling from queue
- `AtomicDbQueuePullResult`: Result of a queue pull operation
- `RaceCondition`: Error class for race condition detection

## License

MIT
