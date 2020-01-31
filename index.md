### Welcome to sst_engine's documentation! 


*sst_engine* is a simple key-value store, powered by SSTables. 

#### Usage 
```
from sst_engine import DB

db = DB() 
db["foo"] = "bar"
assert db["foo"] == "bar"

```


#### Features 



#### Installation 

`pip3 install <todo>` 



#### API

* `get`(key) =>

* `set(key)` =>

* `range()` =>

* `delete(key)` =>

* `contains(key)` =>



#### Design & Implementation 

Th design philoshopy is essentially a simplified version of levelDB. 

Every write is initially inserted into an in-memory data structure (typically called "memtable)
 -- in this case,  red-black tree. 
 
When the memtable's size exceeds a certain threshold, all entries are written out into a segment file. 
Due to the nature of the internal data-structure for the memtable, all entries will be written in sorted order:
the resulting file is termed a sorted-string table (SST).

Whilst performing the above write, we also maintain a sparse index table, keeping track of the 
file offset of every in 1 in x entries. The reason is to enable faster reads:

When a read comes in, we first look into the memtable for the corresponding k-v pair; if it doesn't exist, 
we look at the *closest* entry (by key) in the sparse table. We jump to the file offset of that entry and linearly scan forwards into 
the file until we find the desire key-value pair. This is only possible because the file is an SSTable sorted by key. 
A good sparse offset should ensure that the time complexity for reads is on average `O(log(n))`

Periodically, the segments are merged (also called "compaction"); this ensures a reduction 
in memory footprint by removing old entries and thus decreasing the number of segments. 

An addition optimisation includes the use of bloom-filters to check if a key is not present in 
the DB. The naive alternative is to look into tree first, then every single segment; this has a much worse 
time complexity. 








#### Contribute
Visit <url>  to submit issues and open PRs

