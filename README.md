## JumpDB

JumpDB is a simple key-value store that exploits Sorted String Tables.

Here's a [tutorial](https://navyazaveri.github.io/algorithms/2020/01/12/write-a-kv-store-from-scratch.html)  which goes a little more in-depth into how it works.



### Install  



### Usage 

```
from jumpDB import DB

db = DB(max_inmemory_size=2, persist_segments=False)
db["k1"] = "v1"
db["k2"] = "v2"
del db["k2"]
assert db["k1"] == "v1"
assert "k2" not in db
```


### API

* `get(key)` => Finds the corresponding value to the given key 

* `set(key, value)` => Insert the entry into the db 

* `delete(key)` => Deletes the key from the db 

* `contains(key)` => Checks if the given key is present in the db 



### Design & Implementation 

Th design is essentially a simplified version of [levelDB](https://en.wikipedia.org/wiki/LevelDB). 

Every write is initially inserted into an in-memory data structure (typically called "memtable")
 -- in this case,  a red-black tree. 
 
When the memtable's size exceeds a certain threshold, all entries are written out into a segment file. 
Exploiting the properties of a red-black BST, we can ensure all entries will be efficiently written in sorted order.
The resulting file is immutable and called a sorted-string table (SST).

Whilst performing the above write, we also maintain a sparse index table, keeping track of the 
file offset of every in 1 in x entries. 

When a read comes in, we first look into the memtable for the corresponding k-v pair; if it doesn't exist, 
we look at the *closest* entry (by key) in the sparse table. We *jump* to the file offset of that entry and then linearly scan forwards 
 until we find the desired key-value pair. This is only possible because the SST is sorted by key.
 
 
Periodically, the segments are merged (also called "compaction"); this ensures a reduction 
in memory footprint as the resulting merged segments(s) would only hold the most recent entries. 

An addition optimisation includes the use of bloom-filters to check if a key is not present in 
the DB. This saves us from performing heavy disk reads for keys that haven't been inserted into the db. 



### Tests 
Run `pytest -v`


### License 
BSD 2-Clause License
