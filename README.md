# Yuna DB: `dict`-like semantics for LMDB

## Introduction

Yuna is a key/value store that's built upon LMDB (the Symas Lightning
Memory-mapped Database).  LMDB really is lightning-fast, but as a C
library only allows you to lookup a byte string value, using either
an integer key or a byte string key.

Yuna provides semantics similar to a `dict`.  You can specify a serialization
format, and optionally a compression format to use, and Yuna will serialize
values and write them to the database as byte strings.  It will
automatically recover the value from the bytes string.

For example, if `my_value` is any Python value that can be serialized by the chosen
serialization format in table `foo`, this would work:

`db.tables.foo.put(my_key, my_value)`

After putting the value you can get it again:

`my_value = db.tables.foo.get(my_key)`

And of course you can delete it:

`db.tables.foo.delete(my_key)`


If you want to use `dict` syntax, that's supported as well:

```
db.tables.foo[my_key] = my_value
my_value = db.tables.foo[my_key]
del db.tables.foo[my_key]
```

## Example

```
from yuna import Yuna, SERIALIZE_JSON, SERIALIZE_STR

with Yuna("/tmp/test.ydb", create=True) as db:
    db.new_table("names", serialize=SERIALIZE_JSON)

    key = "feynman"
    value = dict(first_name="Richard", last_name="Feynman")
    db.tables.names.put(key, value)

    temp = db.tables.names.get(key)
    assert value == temp

    tbl_abbrevs = db.new_table("abbrevs", serialize=SERIALIZE_STR)
    key = "l8r"
    value = "see you later"

    tbl_abbrevs[key] = value
    assert value == tbl_abbrevs[key]
```

## Planned new features to come:

* Support integer keys with a `.insert()` method providing autoincrement
* Finish the Zstandard compression support
* Add a REPL (Python with the yuna module already loaded and the DB open)
* Add lots more unit tests


## Advantages of Yuna over writing your own serialization code by hand
* **Much more convenient.**  Using LMDB directly and writing your own serialization code
  means having to write some very similar boilerplate code, over and over.  For example,
  for a table where the data is serialized as JSON and compressed with LZ4:

```
# See LMDB documentation for details of transaction and get
with lmdb_env.begin(db="foo") as txn:
    temp = txn.get(my_key, default=None)
if temp is None:
    result = my_default_value
else:
    result = deserialize_json(decompress_lz4(temp))
```

The above is replaced by one line:

```
result = db.tables.foo.get(my_key, my_default_value)
```

* **Looser coupling of code and data.**  Yuna reads the database file
  to find out what form of serialization is being used, and what form
  of compression (if any) is being used.  If you change your mind, you
  only have to change the code that generates the Yuna file; the code
  that uses the database doesn't have to change.

  For example, if last week you were serializing in JSON and not compressing,
  and this week you are serializing in MesgPack and compressing with LZ4,
  the code that uses the database doesn't change at all.  And in fact
  you can switch between loading this week's file and last week's file
  without having to change your code.

  Even if you wrote your own wrapper functions to reduce the amount of
  boilerplate in your code, you would have to make sure to call the correct
  wrapper for each table.  For example, if one table is compressed and
  another is not, you would need a different wrapper for each, even if
  they both used the same serialization format.

* **Standardized metadata allows standardized tools.**  Yuna will soon
  include a tool that will read the metadata and write a terse summary of
  what's in the file.  Yuna also offers a standard "name" and "version" feature,
  which Yuna will check if you use them.  If you accidentally load the wrong
  file, it's better to get an immediate failure with a useful error message
  instead of getting a runtime error because an expected table wasn't present in
  the database file.  Yuna raises an exception with a message like this:

  `LMDB file 'name' mismatch: expected 'foo', got 'bar'`

  When you make a breaking change in your database file format, you can
  change the 'version' number, and get a similar exception if you accidentally
  load an old, outdated file:

  `LMDB file 'version' mismatch: expected 3, got 2`
