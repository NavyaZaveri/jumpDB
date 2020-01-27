

## Write a simple kv store from first principles.

This is a simple primer I hope would be useful to those wanting 
. The aim is to build to build your own key-value store from first principles up to a "critical mass" that 
you can later extend and 

#### Motivation

A hashamp can hold enriees in memory with a capacity bound by RAM.
Sometimes that bound isn't enough; we'd like a "bigger" hashmap -- one that's bound 
by disk-space.

First, let's establish some performance benchamrks for `get()`, and `set()` operations



A naive, inefficient solution to sole the problem something like the following: 

* Insert an entry into a hashmap []()

* As soon as  the capacity reaches, RAM dump all entries into a file and clear the in-memory map. 

* For a `get()` query, lookup the hashamp first; if the key isn't present, look into all existing files for the key-value pair

* Repeat (1) 

This works and is relatively easy to implement, but is quite inefficient. If the user performs and `get()` and the key isn't present in the 
hashamp, we'd have to linearly scan into every file to search for the entry. So if there are `n` entries in total,
that means a `get()` would take an average of  `O(n)` 



 
#### Design & Implementation

 
  