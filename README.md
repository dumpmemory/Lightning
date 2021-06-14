# Lightning containers
Set of lock-free data structures

* Hash map
* Linked hash map
* Page linked list 

The intention of this project is to provide variety of commonly seen shared data structures for
performance senstive applications. All of the data structures in provided in the list are lock-free and almost wait-free. The hash map is lightning fast and possibly the fastest according to my benchmark.

## HashMap
The design of the hash map is derived from Cliff Click's talk on Stanford University. It is linear scalable on concurrent workloads even under very high contentions. All operations, including insert, read, update and remove are lock-free, including case of internal buffer resize. The data structure composites of a few atomic variables and one or two large buffer on the heap. The atomic variables are used for statistics and pointers for the buffers and the buffers contains data. Each key-value entry is a double word pair in the buffer and the data structure act on it by issuing seperate atomic operations. The hash map scans key-value entry by stride 1 atomic load, which implicits ideal cache locality and have the potential to be optimized further by prefetching. 

### Resize
In case of the buffer cannot hold the key-value entries within optimal fill rate, the buffer would resize. The resize process is also lock-free and passive parallelism. If one thread found out that the fill rate goes exceeds optimal, it will create a new buffer with size doubled and start to migrate key-value pairs from the old buffer to the new buffer before insertion. During the migration, new entries will be inserted to new buffer, and read from old buffer first and then the new buffer. Each of the operations have the need to scan the old buffer, will migrate the validated pairs it scanned to the new buffer. 

### Contentions
The hash map has considerable smaller performance degrade under high contention workloads. This is because of the cost for each of the contention is low and mostly scattered across the buffer. The shared statistics atomic variables are eventual consistent and the buffer pointers only changes except resize. Strong ordering operates on key-value entry buffers by key basics, which the only contention occurrence is hash collision and key contention, both of them can be resolved by a few atomic operations.

### Mapping Types
To maximizing the throughtput and avoid unnecessary instructions, lightning provide various flavours for different use cases. 
* **HashMap** provides generic `K` to `V` mapping.
* **ObjectMap** priovides `usize` to generic `K` mapping
* **WordMap** provides `usize` to `usize` mapping

Since atomic operations only works on `usize`, supporting generic key and value types needs considerable extra works that does not need for `WordMap` scenarios. Generic hash map has attachment buffer alone with key-value pair buffer, to be used as the container for generic typed key and values. When attachment buffers are used, original key-value buffers are used for hash value and markers and keys in attachment buffers will be checked against the lookup key in case of `K` to `V` mapping. Both of the `K` and `V` types must implement `Clone` trait, because the key and values will be simply to be cloned into or out of the attachment buffer.

### Entry-wise Mutes and Read-write Locks
Some use cases may require to take lock on an entry in the hash map. Lightning provides this a litte additional features by using the under utilizerd spece in its entry buffers. Typically, locking on individual entries in the hash map requires put the lock itself and the value protected by the lock wrapped by `Arc` on the heap. This is wasteful when lightning is able to achieve the same without allocating additional space for the locks itself, but using its internal data structures only. The hash maps provides following types of locks.
*  **HashMap** provides read-write lock
*  **ObjectMap** provides read-write lock
*  **WordMap** provides mutex

All of the locks provided by lightning are spin-locks. They are designed to used for low key contentions scenarios. If the use case expects high contentions on specific keys, consider using more complex locks wrapped by `Arc` instead. 

### Performance
Benchmarks are besed on the throughput in terms of thread count. The test platform has following hardware
* OS: Ubuntu 20.04.2
* CPU: AMD Threadripper 3990X @ 2.2GHz
* RAM: 256GB @ 3200MTS
* Rust: 1.54.0-nightly
  
Test data are synthesized each time before the test according to the workload and contentions for the test cases, total of `100663296` operations.

![](doc/benchmark.jpg)

Results shows that lightning have the best throughput across 1 to 128 threads and almost always scalable, expecially for insertions. A more comprehensive study on other hardwares is still in progress and will be released when it is ready.
