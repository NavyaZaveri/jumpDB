## JumpDB

JumpDB is a simple key-value store that exploits Sorted String Tables.

I've written a [tutorial](https://navyazaveri.github.io/algorithms/2020/01/12/write-a-kv-store-from-scratch.html)  which goes a little more in-depth into how it works.

### Install  



### Usage 

```
from jumpDB import DB

db = DB(max_inmemory_size=2, persist_segments=False)
db["k1"] = "v1"
db["k2"] = "v2"
db["k3"] = "v3"
assert db["k1"] == "v1"
assert db["k2"] == "v2"
assert db["k3"] == "v3"
```


### API

* `get`(key) => Finds the corresponding value to the given key 

* `set(key, value)` => Insert the entry into the db 

* `delete(key)` => Deletes the key from the db 

* `contains(key)` => Checks if the given key is present in the db 



### License 
BSD 2-Clause License
