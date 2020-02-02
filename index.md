### Welcome jumpDB's documentation! 


*jumpDB* is a simple key-value store, powered by SSTables. 

#### Usage 
```
from jumpDB import DB

db = DB() 
db["foo"] = "bar"
assert db["foo"] == "bar"

```




#### Installation 

`pip3 install <todo>` 



#### API

* `get`(key) => Finds the corresponding value to the given key 

* `set(key, value)` => Insert the entry into the db 

* `range()` =>

* `delete(key)` => Deletes the key from the db 

* `contains(key)` => Checks if the given key is present in the db 



#### Design & Implementation 

Th design philosophy is essentially a simplified version of levelDB. 

Every write is initially inserted into an in-memory data structure (typically called "memtable")
 -- in this case,  red-black tree. 
 
When the memtable's size exceeds a certain threshold, all entries are written out into a segment file. 
Due to the nature of the internal data-structure for the memtable, all entries will be written in sorted order:
the resulting file is termed a sorted-string table (SST).

Whilst performing the above write, we also maintain a sparse index table, keeping track of the 
file offset of every in 1 in x entries. 

When a read comes in, we first look into the memtable for the corresponding k-v pair; if it doesn't exist, 
we look at the *closest* entry (by key) in the sparse table. We jump to the file offset of that entry and linearly scan forwards into 
the file until we find the desire key-value pair. This is only possible because the file is sorted by key. 

Periodically, the segments are merged (also called "compaction"); this ensures a reduction 
in memory footprint by removing old entries and thus decreasing the number of segments. 

An addition optimisation includes the use of bloom-filters to check if a key is not present in 
the DB. This saves from performing heavy disk reads for keys that haven't been inserted into the db. 



#### Contribute
Visit https://github.com/NavyaZaveri/jumpDB/ to submit issues and open PRs.


