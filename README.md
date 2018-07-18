# FastqMultiThreadProducerConsumer
Example program for efficient multithread producer-consumer I/O with kseq

Uses kseq (from Heng Li) and moodycamel's ConcurrentQueue.

c++11 OpenMP and zlib are used in this example.

TODO: Blocking version rather than spinlocks to conserve CPU.
