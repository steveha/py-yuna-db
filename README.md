Yuna DB: dict-like semantics for LMDB

Yuna is a key/value store that's built upon LMDB (the Symas Lightning
Memory-mapped Database).  LMDB really is lightning-fast, but as a C
library only allows you to lookup a byte string value, using either
an integer key or a byte string key.

Yuna provides semantics similar to a dict.  You can specify a serialization
format, and optionally a compression format to use, and Yuna will serialize
values and write them to the database as byte strings.  It will
automatically recover the value from the bytes string.

For example, if x is any Python value that can be serialized by the chosen
serialization format in table foo, this would work:

`db.tables.foo.put("x_value", x)`

After that .put() you can call .get():

`x = db.tables.foo.get("x_value")`


# Planned new features to come:

* Implement a context manager on the Yuna class
* Support integer keys
* Finish the Zstandard compression support
* Add iterators to quickly find keys within a specified range
* Add a REPL (Python with the yuna module already loaded and the DB open)
