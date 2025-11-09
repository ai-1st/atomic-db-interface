import test from 'ava'
import {
  AtomicMemoryDb,
  AtomicDbItemKey,
  AtomicDbItem,
  RaceCondition,
  AtomicLRUCache,
} from './index'

const db = new AtomicMemoryDb()

test('basic CRUD operations', async (t) => {
  const key = { pk: 'test-crud', sk: 'item1' }
  await db.delete(key)

  // Test set and get
  const item = {
    ...key,
    data: { message: 'hello' },
  }
  await db.set(item)
  const result = await db.get(key)
  t.deepEqual(result?.data, item.data)

  // Test delete
  await db.delete(key)
  const deleted = await db.get(key)
  t.is(deleted, undefined)
})

test('batch operations', async (t) => {
  const keys = [
    { pk: 'test-batch', sk: 'item1' },
    { pk: 'test-batch', sk: 'item2' },
    { pk: 'test-batch', sk: 'item3' },
  ]
  await db.delete(keys)

  // Test batch set
  const items = keys.map((key) => ({
    ...key,
    data: {
      index: parseInt(key.sk.replace('item', '')),
    },
  }))
  await db.set(items)

  // Test batch get - should maintain order
  const results = await db.getMany(keys)
  t.deepEqual(
    results.map((r) => r?.data),
    items.map((i) => i.data)
  )

  // Test get with missing items
  const mixedKeys = [
    keys[0],
    { pk: 'test-batch', sk: 'missing' },
    keys[2],
  ]
  const mixedResults = await db.getMany(mixedKeys)
  t.deepEqual(
    mixedResults.map((r) => r?.data),
    [items[0].data, undefined, items[2].data]
  )

  // Test batch delete
  await db.delete(keys)
  const deletedResults = await db.getMany(keys)
  t.deepEqual(deletedResults, [
    undefined,
    undefined,
    undefined,
  ])
})

test('query operations', async (t) => {
  const pk = 'test-query'
  const keys = [
    { pk, sk: 'a:item1' },
    { pk, sk: 'a:item2' },
    { pk, sk: 'b:item1' },
  ]
  await db.delete(keys)

  // Set up test data
  const items = keys.map((key) => ({
    ...key,
    data: { category: key.sk.split(':')[0] },
  }))
  await db.set(items)

  t.deepEqual(
    [
      // Test query with prefix
      (await db.query({ pk, sk: 'a:' })).length,
      (await db.query({ pk })).length,
      (await db.query({ pk, sk: 'c:' })).length,
    ],
    [2, 3, 0]
  )

  await db.delete(keys)
})

test('atomic operations with race conditions', async (t) => {
  const itemKey = {
    pk: 'test',
    sk: 'profile',
  }
  const lockKey = {
    pk: 'test',
    sk: 'profile_lock',
  }

  // Get initial lock
  const lock = await db.getLock(lockKey)

  // Set initial value
  await db.setAtomic(
    {
      pk: itemKey.pk,
      sk: itemKey.sk,
      data: { name: '' },
    },
    lock
  )

  // Simulate two parallel operations trying to set different names
  const operation1 = async () => {
    const lock1 = await db.getLock(lockKey)

    // Add a longer delay for operation1 to ensure race condition
    await new Promise((resolve) =>
      setTimeout(resolve, 500)
    )

    await db.setAtomic(
      {
        pk: itemKey.pk,
        sk: itemKey.sk,
        data: { name: 'John' },
      },
      lock1
    )
    return 'John'
  }

  const operation2 = async () => {
    // Add a small delay before getting lock for operation2
    await new Promise((resolve) =>
      setTimeout(resolve, 100)
    )

    const lock2 = await db.getLock(lockKey)

    // Add a small delay before setting for operation2
    await new Promise((resolve) =>
      setTimeout(resolve, 400)
    )

    await db.setAtomic(
      {
        pk: itemKey.pk,
        sk: itemKey.sk,
        data: { name: 'Mary' },
      },
      lock2
    )
    return 'Mary'
  }

  // Run both operations concurrently
  const results = await Promise.allSettled([
    operation1(),
    operation2(),
  ])

  // Verify that exactly one operation succeeded and one failed
  const succeeded = results.filter(
    (r) => r.status === 'fulfilled'
  ).length
  const failed = results.filter(
    (r) => r.status === 'rejected'
  ).length
  t.is(
    succeeded,
    1,
    'Exactly one operation should succeed'
  )
  t.is(
    failed,
    1,
    'Exactly one operation should fail'
  )

  // Verify that the failed operation was due to RaceCondition
  const failedOp = results.find(
    (r) => r.status === 'rejected'
  ) as PromiseRejectedResult
  t.true(
    failedOp.reason instanceof RaceCondition,
    'Failed operation should throw RaceCondition'
  )

  // Get the name that succeeded
  const successOp = results.find(
    (r) => r.status === 'fulfilled'
  ) as PromiseFulfilledResult<string>
  const expectedName = successOp.value

  // Verify that exactly one name was set
  const finalItem = await db.get(itemKey)
  t.is(
    finalItem?.data?.name,
    expectedName,
    'Name should match the successful operation'
  )
  t.true(
    ['John', 'Mary'].includes(
      finalItem?.data?.name || ''
    ),
    'Name should be either John or Mary'
  )

  await db.delete([lockKey, itemKey])
})

test('stream operations', async (t) => {
  const pk = 'test-stream'
  const keys = [
    { pk, sk: 'item1' },
    { pk, sk: 'item2' },
    { pk, sk: 'item3' },
  ]
  await db.delete(keys)

  // Set up test data
  const items = keys.map((key) => ({
    ...key,
    data: {
      index: parseInt(key.sk.replace('item', '')),
    },
  }))
  await db.set(items)

  // Test streaming
  const stream = db.stream({ pk })
  const streamedItems: any[] = []

  await new Promise((resolve, reject) => {
    stream.on('data', (item) => {
      streamedItems.push(item)
    })
    stream.on('end', resolve)
    stream.on('error', reject)
  })

  t.deepEqual(
    [
      streamedItems.length,
      streamedItems.map((i) => i.data),
    ],
    [items.length, items.map((i) => i.data)]
  )

  await db.delete(keys)
})

test('large batch operations', async (t) => {
  // Create 50 items (2 full batches)
  const keys = Array.from(
    { length: 50 },
    (_, i) => ({
      pk: 'test-large-batch',
      sk: `item${i.toString().padStart(2, '0')}`, // item00, item01, etc.
    })
  )
  await db.delete(keys)

  // Test batch set
  const items = keys.map((key) => ({
    ...key,
    data: {
      index: parseInt(key.sk.replace('item', '')),
    },
  }))
  await db.set(items)

  // Test batch get - should maintain order
  const results = await db.getMany(keys)
  t.deepEqual(
    results.map((r) => r?.data),
    items.map((i) => i.data)
  )

  // Test get with missing items
  const mixedKeys = [
    keys[0],
    { pk: 'test-large-batch', sk: 'missing1' },
    keys[25], // middle of second batch
    { pk: 'test-large-batch', sk: 'missing2' },
    keys[49], // last item
  ]
  const mixedResults = await db.getMany(mixedKeys)
  t.deepEqual(
    mixedResults.map((r) => r?.data),
    [
      items[0].data,
      undefined,
      items[25].data,
      undefined,
      items[49].data,
    ]
  )

  // Test batch delete
  await db.delete(keys)
  const deletedResults = await db.getMany(keys)
  t.deepEqual(
    deletedResults,
    Array(50).fill(undefined)
  )
})

test('LRU cache basic operations', async (t) => {
  const memoryDb = new AtomicMemoryDb()
  const cachedDb = new AtomicLRUCache(memoryDb, 2) // Small cache size to test eviction

  // Test cache miss and fill
  const key1 = { pk: 'test-cache', sk: 'item1' }
  const item1 = { ...key1, data: { value: 1 } }
  await memoryDb.set(item1)

  const result1 = await cachedDb.get(key1)
  t.deepEqual(
    result1,
    item1,
    'Should fetch and cache item on miss'
  )

  // Test cache hit
  const result2 = await cachedDb.get(key1)
  t.deepEqual(
    result2,
    item1,
    'Should return cached item'
  )

  // Test cache eviction
  const key2 = { pk: 'test-cache', sk: 'item2' }
  const item2 = { ...key2, data: { value: 2 } }
  await memoryDb.set(item2)
  await cachedDb.get(key2)

  const key3 = { pk: 'test-cache', sk: 'item3' }
  const item3 = { ...key3, data: { value: 3 } }
  await memoryDb.set(item3)
  await cachedDb.get(key3)

  // key1 should be evicted, forcing a DB read
  const result4 = await cachedDb.get(key1)
  t.deepEqual(
    result4,
    item1,
    'Should fetch from DB after eviction'
  )
})

test('LRU cache getMany operation', async (t) => {
  const memoryDb = new AtomicMemoryDb()
  const cachedDb = new AtomicLRUCache(memoryDb, 3)

  const keys = [
    { pk: 'test-cache', sk: 'batch1' },
    { pk: 'test-cache', sk: 'batch2' },
    { pk: 'test-cache', sk: 'batch3' },
  ]

  const items = keys.map((key, i) => ({
    ...key,
    data: { value: i + 1 },
  }))

  await memoryDb.set(items)

  // First getMany should cache all items
  const results1 = await cachedDb.getMany(keys)
  t.deepEqual(
    results1,
    items,
    'Should fetch and cache all items'
  )

  // Modify one item directly in the DB
  const modifiedItem = {
    ...items[1],
    data: { value: 20 },
  }
  await memoryDb.set(modifiedItem)

  // Second getMany should return cached values
  const results2 = await cachedDb.getMany(keys)
  t.deepEqual(
    results2,
    items,
    'Should return cached items even if DB was modified'
  )

  // After set through cache, should return new values
  await cachedDb.set(modifiedItem)
  const results3 = await cachedDb.getMany(keys)
  const expectedResults3 = [...items]
  expectedResults3[1] = modifiedItem
  t.deepEqual(
    results3,
    expectedResults3,
    'Should return updated item after cache update'
  )
})

test('LRU cache set operations', async (t) => {
  const memoryDb = new AtomicMemoryDb()
  const cachedDb = new AtomicLRUCache(memoryDb, 3)

  const key = { pk: 'test-cache', sk: 'set1' }
  const item1 = { ...key, data: { value: 1 } }

  // Set should update cache
  await cachedDb.set(item1)
  const result1 = await cachedDb.get(key)
  t.deepEqual(
    result1,
    item1,
    'Cache should be updated after set'
  )

  // Modify directly in memory DB
  const item2 = { ...key, data: { value: 2 } }
  await memoryDb.set(item2)

  // Should still return cached value
  const result2 = await cachedDb.get(key)
  t.deepEqual(
    result2,
    item1,
    'Should return cached value even if DB was modified'
  )

  // Update through cache
  const item3 = { ...key, data: { value: 3 } }
  await cachedDb.set(item3)

  // Both cache and DB should have new value
  const cacheResult = await cachedDb.get(key)
  const dbResult = await memoryDb.get(key)
  t.deepEqual(
    cacheResult,
    item3,
    'Cache should have updated value'
  )
  t.deepEqual(
    dbResult,
    item3,
    'DB should have updated value'
  )
})

test('LRU cache atomic operations', async (t) => {
  const memoryDb = new AtomicMemoryDb()
  const cachedDb = new AtomicLRUCache(memoryDb, 3)

  const key = { pk: 'test-cache', sk: 'atomic1' }
  const item1 = { ...key, data: { value: 1 } }

  // Set initial value
  await cachedDb.set(item1)

  // Get lock for atomic update
  const lock = await cachedDb.getLock(key)

  // Update with atomic operation
  const item2 = { ...key, data: { value: 2 } }
  await cachedDb.setAtomic(item2, lock)

  // Both cache and DB should have new value
  const cacheResult = await cachedDb.get(key)
  const dbResult = await memoryDb.get(key)
  t.deepEqual(
    cacheResult,
    item2,
    'Cache should have atomically updated value'
  )
  t.deepEqual(
    dbResult,
    item2,
    'DB should have atomically updated value'
  )

  // Try atomic update with wrong lock version
  const item3 = { ...key, data: { value: 3 } }
  await t.throwsAsync(
    async () => {
      await cachedDb.setAtomic(item3, lock)
    },
    { instanceOf: RaceCondition },
    'Should throw RaceCondition on version mismatch'
  )
})

test('LRU cache query and stream operations', async (t) => {
  const memoryDb = new AtomicMemoryDb()
  const cachedDb = new AtomicLRUCache(memoryDb, 3)

  const items = [
    {
      pk: 'test-cache',
      sk: 'query1',
      data: { value: 1 },
    },
    {
      pk: 'test-cache',
      sk: 'query2',
      data: { value: 2 },
    },
    {
      pk: 'test-cache',
      sk: 'query3',
      data: { value: 3 },
    },
  ]

  await cachedDb.set(items)

  // Query should bypass cache
  const queryResults = await cachedDb.query({
    pk: 'test-cache',
    sk: 'query',
  })
  t.deepEqual(
    queryResults,
    items,
    'Query should return all items'
  )

  // Modify items directly in DB
  const modifiedItems = items.map((item) => ({
    ...item,
    data: { value: item.data.value * 10 },
  }))
  await memoryDb.set(modifiedItems)

  // Query should reflect DB changes
  const queryResults2 = await cachedDb.query({
    pk: 'test-cache',
    sk: 'query',
  })
  t.deepEqual(
    queryResults2,
    modifiedItems,
    'Query should return modified items from DB'
  )

  // But individual gets should still use cache
  const cachedItem = await cachedDb.get(items[0])
  t.deepEqual(
    cachedItem,
    items[0],
    'Get should return cached value after query'
  )

  // Stream should also bypass cache
  const streamItems: AtomicDbItem[] = []
  const stream = cachedDb.stream({
    pk: 'test-cache',
    sk: 'query',
  })

  await new Promise<void>((resolve, reject) => {
    stream.on('data', (item: AtomicDbItem) => {
      streamItems.push(item)
    })
    stream.on('end', resolve)
    stream.on('error', reject)
  })

  t.deepEqual(
    streamItems,
    modifiedItems,
    'Stream should return modified items from DB'
  )
})

test('queue basic push/pull/acknowledge flow', async (t) => {
  const queuePk = 'test-queue-basic'

  // Push items to queue
  await db.queuePush([
    {
      pk: queuePk,
      sk: 'item-1',
      data: { message: 'first' },
    },
    {
      pk: queuePk,
      sk: 'item-2',
      data: { message: 'second' },
    },
    {
      pk: queuePk,
      sk: 'item-3',
      data: { message: 'third' },
    },
  ])

  // Pull first item
  const result1 = await db.queuePull({
    pk: queuePk,
  })
  t.truthy(result1.item, 'Should pull an item')
  t.is(
    result1.item?.data.message,
    'first',
    'Should pull first item (FIFO)'
  )

  // Try to pull again while first is processing - should return empty (strict FIFO)
  const result2 = await db.queuePull({
    pk: queuePk,
  })
  t.is(
    result2.item,
    undefined,
    'Should return empty while first item is processing (strict FIFO)'
  )

  // Acknowledge first item
  await db.queueAcknowledge({
    pk: result1.item!.pk,
    sk: result1.item!.sk,
  })

  // Now can pull second item
  const result2AfterAck = await db.queuePull({
    pk: queuePk,
  })
  t.truthy(
    result2AfterAck.item,
    'Should pull second item after first is acknowledged'
  )
  t.is(
    result2AfterAck.item?.data.message,
    'second',
    'Should pull second item (FIFO)'
  )

  // Verify first item is deleted
  const check = await db.get({
    pk: result1.item!.pk,
    sk: result1.item!.sk,
  })
  t.is(
    check,
    undefined,
    'First item should be deleted'
  )

  // Acknowledge second item
  await db.queueAcknowledge({
    pk: result2AfterAck.item!.pk,
    sk: result2AfterAck.item!.sk,
  })

  // Pull third item
  const result3 = await db.queuePull({
    pk: queuePk,
  })
  t.is(
    result3.item?.data.message,
    'third',
    'Should pull third item'
  )

  // Acknowledge third item
  await db.queueAcknowledge({
    pk: result3.item!.pk,
    sk: result3.item!.sk,
  })

  // Queue should be empty
  const result4 = await db.queuePull({
    pk: queuePk,
  })
  t.is(
    result4.item,
    undefined,
    'Queue should be empty'
  )
})

test('queue FIFO ordering with explicit sk', async (t) => {
  const queuePk = 'test-queue-ordering'

  // Push items with explicit sk
  await db.queuePush({
    pk: queuePk,
    sk: 'item-1',
    data: 1,
  })
  await new Promise((resolve) =>
    setTimeout(resolve, 10)
  ) // Small delay for ULID ordering
  await db.queuePush({
    pk: queuePk,
    sk: 'item-2',
    data: 2,
  })
  await new Promise((resolve) =>
    setTimeout(resolve, 10)
  )
  await db.queuePush({
    pk: queuePk,
    sk: 'item-3',
    data: 3,
  })

  // Pull items in order
  const results: number[] = []
  for (let i = 0; i < 3; i++) {
    const result = await db.queuePull({
      pk: queuePk,
    })
    if (result.item) {
      results.push(result.item.data)
      await db.queueAcknowledge({
        pk: result.item.pk,
        sk: result.item.sk,
      })
    }
  }

  t.deepEqual(
    results,
    [1, 2, 3],
    'Items should be pulled in FIFO order'
  )
})

test('queue with custom sk maintains order', async (t) => {
  const queuePk = 'test-queue-custom-sk'

  // Push items with custom sk
  await db.queuePush([
    { pk: queuePk, sk: 'task-001', data: 'A' },
    { pk: queuePk, sk: 'task-002', data: 'B' },
    { pk: queuePk, sk: 'task-003', data: 'C' },
  ])

  // Pull items
  const results: string[] = []
  for (let i = 0; i < 3; i++) {
    const result = await db.queuePull({
      pk: queuePk,
    })
    if (result.item) {
      results.push(result.item.data)
      await db.queueAcknowledge({
        pk: result.item.pk,
        sk: result.item.sk,
      })
    }
  }

  t.deepEqual(
    results,
    ['A', 'B', 'C'],
    'Items should maintain custom sk order'
  )
})

test('queue strict FIFO prevents pulling next item until previous is ack', async (t) => {
  const queuePk = 'test-queue-locks'

  await db.queuePush([
    { pk: queuePk, sk: 'item1', data: 1 },
    { pk: queuePk, sk: 'item2', data: 2 },
  ])

  // Pull first item
  const result1 = await db.queuePull({
    pk: queuePk,
  })
  t.is(
    result1.item?.data,
    1,
    'First pull gets item1'
  )

  // Try to pull again - should return empty (strict FIFO - can't pull next until first is done)
  const result2 = await db.queuePull({
    pk: queuePk,
  })
  t.is(
    result2.item,
    undefined,
    'Should return empty while first item is processing (strict FIFO)'
  )

  // Acknowledge first item
  await db.queueAcknowledge({
    pk: result1.item!.pk,
    sk: result1.item!.sk,
  })

  // Now can pull second item
  const result3 = await db.queuePull({
    pk: queuePk,
  })
  t.is(
    result3.item?.data,
    2,
    'Can pull item2 after item1 is acknowledged'
  )

  // Clean up
  await db.queueAcknowledge({
    pk: result3.item!.pk,
    sk: result3.item!.sk,
  })
})

test('queue lock TTL expiration makes item available again', async (t) => {
  const queuePk = 'test-queue-ttl'

  await db.queuePush({
    pk: queuePk,
    sk: 'item1',
    data: 'test',
  })

  // Pull with 1 second TTL
  const result1 = await db.queuePull({
    pk: queuePk,
    ttlSeconds: 1,
  })
  t.truthy(result1.item, 'Item should be pulled')

  // Try to pull immediately - should be locked
  const result2 = await db.queuePull({
    pk: queuePk,
  })
  t.is(
    result2.item,
    undefined,
    'Item should be locked'
  )

  // Wait for lock to expire
  await new Promise((resolve) =>
    setTimeout(resolve, 1100)
  )

  // Should be able to pull again
  const result3 = await db.queuePull({
    pk: queuePk,
  })
  t.truthy(
    result3.item,
    'Item should be available after lock expires'
  )
  t.is(
    result3.item?.data,
    'test',
    'Should get same item'
  )

  // Clean up
  await db.queueAcknowledge({
    pk: result3.item!.pk,
    sk: result3.item!.sk,
  })
})

test('queue multiple consumers process items sequentially (strict FIFO)', async (t) => {
  const queuePk = 'test-queue-multi'

  // Push 5 items
  await db.queuePush([
    { pk: queuePk, sk: 'item-1', data: 1 },
    { pk: queuePk, sk: 'item-2', data: 2 },
    { pk: queuePk, sk: 'item-3', data: 3 },
    { pk: queuePk, sk: 'item-4', data: 4 },
    { pk: queuePk, sk: 'item-5', data: 5 },
  ])

  // Simulate consumers processing items sequentially (strict FIFO)
  // With strict FIFO, only one item can be processed at a time
  const results: number[] = []

  // Process items sequentially - each pull must acknowledge before next can be pulled
  for (let i = 0; i < 5; i++) {
    const result = await db.queuePull({
      pk: queuePk,
    })
    if (result.item) {
      results.push(result.item.data)
      await db.queueAcknowledge({
        pk: result.item.pk,
        sk: result.item.sk,
      })
    }
  }

  // All items should be processed in order
  t.deepEqual(
    results,
    [1, 2, 3, 4, 5],
    'All items should be processed exactly once in FIFO order'
  )

  // Queue should be empty
  const final = await db.queuePull({
    pk: queuePk,
  })
  t.is(
    final.item,
    undefined,
    'Queue should be empty'
  )
})

test('queue empty queue handling', async (t) => {
  const queuePk = 'test-queue-empty'

  // Try to pull from empty queue
  const result = await db.queuePull({
    pk: queuePk,
  })
  t.is(
    result.item,
    undefined,
    'No item from empty queue'
  )
})

test('queue with LRU cache', async (t) => {
  const memoryDb = new AtomicMemoryDb()
  const cachedDb = new AtomicLRUCache(
    memoryDb,
    10
  )
  const queuePk = 'test-queue-cache'

  // Push items through cache
  await cachedDb.queuePush([
    { pk: queuePk, sk: 'item-A', data: 'A' },
    { pk: queuePk, sk: 'item-B', data: 'B' },
    { pk: queuePk, sk: 'item-C', data: 'C' },
  ])

  // Pull and acknowledge through cache
  const results: string[] = []
  for (let i = 0; i < 3; i++) {
    const result = await cachedDb.queuePull({
      pk: queuePk,
    })
    if (result.item) {
      results.push(result.item.data)
      await cachedDb.queueAcknowledge({
        pk: result.item.pk,
        sk: result.item.sk,
      })
    }
  }

  t.deepEqual(
    results,
    ['A', 'B', 'C'],
    'Queue should work through cache'
  )

  // Verify queue is empty
  const final = await cachedDb.queuePull({
    pk: queuePk,
  })
  t.is(
    final.item,
    undefined,
    'Queue should be empty'
  )
})
