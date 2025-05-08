# KVS - Key Value Store 

This is a quick-and-dirty key value store that 
attempts to meet the requirements laid out by
the House interview prompt.

The current model leverages async I/O to allow
for single-process concurrency and future extension
into a primary/worker fork-model, whereby multiple
worker processes can be run to take advantage of 
multiple processors. Threads could have been used, but
I chose simplicity and minimal context switching on a 
single processor system vs. context-switching multiple
threads. 

The architecture is basic and consists of a server, a 
command processor, and a storage engine. The storage engine
exposes a global lock to the protocol layer, and serializes
transactions while providing basic transaction isolation and
re-read capability via buffered writes and deletes per client
session. 

There's a ton of room for improvement. 

Needs:

* WAL for re-entry and recovery
* end-to-end testing through the server
* a client
* more control over isolation levels
* much more

# How to run with docker:

1. docker build -t kvs .
2. docker run -p 9888:8888 kvs
