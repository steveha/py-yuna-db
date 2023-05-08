# -*- coding: utf-8 -*-

"""
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

db.tables.foo.put(key, x)

After that .put() you can call .get():

x = db.tables.foo.get(key)
"""

__version__ = "0.2.3"

import os
import types

from typing import Any, Iterator, Optional

import lmdb


from .lmdb_util import YUNA_DEFAULT_MAX_DB_SIZE, YUNA_DEFAULT_MAX_TABLES
from .lmdb_util import YUNA_DB_META_KEY, YUNA_FILE_EXTENSION

from .lmdb_util import _lmdb_open, _yuna_get_meta, _yuna_new_meta, _yuna_put_meta
from .lmdb_util import _lmdb_reserved_delete, _lmdb_reserved_get, _lmdb_reserved_put
from .lmdb_util import _lmdb_table_drop, _lmdb_table_open, _lmdb_table_truncate
from .lmdb_util import delete_file_or_dir

from . import plugins
from .plugins import _YUNA_NOT_PROVIDED
from .plugins import SERIALIZE_JSON, SERIALIZE_MSGPACK, SERIALIZE_STR
from .plugins import COMPRESS_LZ4, COMPRESS_ZLIB, COMPRESS_ZSTD
from .plugins import serialize_json, deserialize_json
from .plugins import serialize_str, deserialize_str
from .plugins import _empty_string_key_check


class YunaSharedData:
    """
    Private data for Yuna, in a class by itself so it can be shared
    among the multiple classes implementing Yuna.
    """
    def __init__(self, env: lmdb.Environment, tables_map: dict, metadata: dict, read_only: bool):
        self.env = env
        self.tables_map = tables_map
        self.metadata = metadata
        self.read_only = read_only
        self.is_dirty = False


class YunaReservedTable:
    """
    This class provides method functions to get/put values from the
    LMDB reserved table.

    This will be .reserved in the open Yuna instance.
    """
    # This is opinionated code.  You cannot specify a serialization or compression format.
    # If you need to do anything that requires a specific serialization or compression
    # format, create a table and use that.  The reserved table should be mostly left alone.
    # Note that LMDB stores things in the reserved table, and Bad Things would happen if
    # you clobbered one of their special values.  In particular, any name used for a table
    # must not be clobbered.
    #
    # If you somehow have a real need to put something other than JSON into the reserved
    # table, serialize it yourself and use .raw_put() to store it.
    #
    # LMDB lets you have any number of tables; use those and leave the reserved table alone.

    def __init__(self, env: lmdb.Environment):
        self.env = env

    def delete(self, key: str) -> None:
        """
        delete a key/value pair from the reserved table.
        """
        bytes_key = serialize_str(key)
        _lmdb_reserved_delete(self.env, bytes_key)

    def get(self, key: str, default: Any=_YUNA_NOT_PROVIDED) -> Any:
        bytes_key = serialize_str(key)
        bytes_value = _lmdb_reserved_get(self.env, bytes_key, None)
        if bytes_value is None:
            if default is _YUNA_NOT_PROVIDED:
                raise KeyError(key)
            else:
                return default
        value = deserialize_json(bytes_value)
        return value

    def put(self, key: str, value: Any) -> None:
        bytes_key = serialize_str(key)
        bytes_value = serialize_json(value)
        _lmdb_reserved_put(self.env, bytes_key, bytes_value)

    def keys(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        _empty_string_key_check(start)
        _empty_string_key_check(stop)
        bytes_start = serialize_str(start) if (start is not None) else None
        bytes_stop = serialize_str(stop) if (stop is not None) else None
        with self.env.begin() as txn:
            with txn.cursor() as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    for bytes_key, _ in cursor:
                        key = deserialize_str(bytes_key)
                        yield key
                else:
                    for bytes_key, _ in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        key = deserialize_str(bytes_key)
                        yield key

    def raw_delete(self, bytes_key: bytes) -> None:
        _lmdb_reserved_delete(self.env, bytes_key)

    def raw_get(self, bytes_key: bytes, default: Any=_YUNA_NOT_PROVIDED) -> Any:
        bytes_value = _lmdb_reserved_get(self.env, bytes_key, None)
        if bytes_value is None:
            if default is _YUNA_NOT_PROVIDED:
                raise KeyError(key)
            else:
                return default
        return bytes_value

    def raw_put(self, bytes_key: bytes, bytes_value: bytes) -> None:
        _lmdb_reserved_put(self.env, bytes_key, bytes_value)

    def raw_keys(self, bytes_start: Optional[bytes]=None, bytes_stop: Optional[bytes]=None) -> Iterator:
        _empty_string_key_check(bytes_start)
        _empty_string_key_check(bytes_stop)
        with self.env.begin() as txn:
            with txn.cursor() as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    for bytes_key, _ in cursor:
                        yield bytes_key
                else:
                    for bytes_key, _ in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        yield bytes_key

    def raw_items(self, bytes_start: Optional[bytes]=None, bytes_stop: Optional[bytes]=None) -> Iterator:
        _empty_string_key_check(bytes_start)
        _empty_string_key_check(bytes_stop)
        with self.env.begin() as txn:
            with txn.cursor() as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    for bytes_key, bytes_value in cursor:
                        yield bytes_key, bytes_value
                else:
                    for bytes_key, bytes_value in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        yield bytes_key, bytes_value

    def raw_values(self, bytes_start: Optional[bytes]=None, bytes_stop: Optional[bytes]=None) -> Iterator:
        _empty_string_key_check(bytes_start)
        _empty_string_key_check(bytes_stop)
        with self.env.begin() as txn:
            with txn.cursor() as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    for _, bytes_value in cursor:
                        yield bytes_value
                else:
                    for bytes_key, bytes_value in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        yield bytes_value


class YunaTablesMap:
    """
    A trvial class, just used as a container for instances of YunaTable.

    This will be .tables in the open Yuna instance.
    """
    def __iter__(self):
        return iter(vars(self).values())


class YunaTableMetadata:
    def __init__(self,
        name: str,
        key_serialize: Optional[str] = None,
        serialize: Optional[str] = None,
        compress: Optional[str] = None,
    ):
        self.name = name
        self.key_serialize = key_serialize
        self.serialize = serialize
        self.compress = compress


class YunaTableBase:
    # This class exists to document the method functions of a YunaTable.
    #
    # Most of the functions in YunaTable are made by a factory, and will
    # be set to override these functions.  But Python will find and use
    # the docstrings from these functions.  So this class is mainly
    # to provide docstrings for all the functions.
    def raw_delete(self, bytes_key: bytes) -> None:
        """
        Delete a key/value pair from the table using the exact bytes key.

        No key serialization will be performed.
        """
        raise NotImplemented("must override")
    def delete(self, key: str) -> None:
        """
        Delete a key/value pair from the table.
        """
        raise NotImplemented("must override")

    def raw_put(self, bytes_key: bytes, bytes_value: bytes) -> None:
        """
        Put a bytes value to the table using the bytes key.

        No key serialization will be performed.  No value serialization
        or compression will be performed.  The exact bytes key will be used
        to put the exact bytes value into the table.

        If there's already a value in the table it will be overitten.
        """
        raise NotImplemented("must override")

    def put(self, key: str, value: Any) -> None:
        """
        Put a value to the table using the key.

        If there's already a value in the table it will be overitten.
        """
        raise NotImplemented("must override")

    def get(self, key: str, default: Any=_YUNA_NOT_PROVIDED) -> Any:
        """
        Get a value from the table using the key.

        If the key is not present in the table, and a default value
        was provided, returns the default value.

        If the key is not present in the table, and no default value
        was provided, raises KeyError.
        """
        raise NotImplemented("must override")
    def raw_get(self, bytes_key: bytes, default: Optional[bytes]=_YUNA_NOT_PROVIDED) -> Any:
        """
        Get a value from the table using the bytes_key.  This must be the
        exact bytes key value; no key serialization will be performed.

        If the key is not present in the table, and a default value
        was provided, returns the default value.

        If the key is not present in the table, and no default value
        was provided, raises KeyError.
        """
        raise NotImplemented("must override")

    def keys(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        """
        Get an iterator that yields up keys from the table.
        Keys will be yielded in the order of their byte key values
        (i.e. the values of the keys after any serialization).

        If start was provided, the iterator will start on the first key that
        is equal to or greater than the provided start value.

        If stop was provided, the iterator will stop before yielding
        a key that is equal to or greater than the provided stop value.

        For example, if a table included the following key/value pairs:

        a=1, b=2, d=4, e=5

        Then keys(start='c', stop='e') would only yield one key: 'd'
        """
        raise NotImplemented("must override")
    def raw_keys(self, bytes_start: Optional[bytes]=None, bytes_stop: Optional[bytes]=None) -> Iterator:
        """
        Get an iterator that yields up raw keys from the table.  These will
        be the actual byte strings of the keys; no key deserialization
        will be performed.

        If bytes_start was provided, the iterator will start on the first key that
        is equal to or greater than the provided start value.

        If bytes_stop was provided, the iterator will stop before yielding
        a key that is equal to or greater than the provided stop value.

        For example, if a table included the following key/value pairs:

        b'a'=0x01, b'b'=0x02, b'd'=0x04, b'e'=0x05

        Then raw_keys(start=b'c', stop=b'e') would only yield one key: b'd'
        """
        raise NotImplemented("must override")

    def items(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        """
        Get an iterator that yields up key/value pairs from the table.
        Each item will be a tuple of the form (key, value)
        Tuples will be yielded in the order of their keys after serialization.

        If start was provided, the iterator will start on the first key that
        is equal to or greater than the provided start value.

        If stop was provided, the iterator will stop before yielding
        a key that is equal to or greater than the provided stop value.

        For example, if a table included the following key/value pairs:

        a=1, b=2, d=4, e=5

        Then items(start='c', stop='e') would only yield one tuple: ('d', 4)
        """
        raise NotImplemented("must override")
    def raw_items(self, bytes_start: Optional[bytes]=None, bytes_stop: Optional[bytes]=None) -> Iterator:
        """
        Get an iterator that yields up raw key/value pairs from the table.
        Each item will be a tuple of the form (bytes_key, bytes_value)
        Tuples will be yielded in the order of their byte keys.

        No key deserialization will be performed.  No value deserialization
        or decompression will be performed.

        If start was provided, the iterator will start on the first key that
        is equal to or greater than the provided start value.

        If stop was provided, the iterator will stop before yielding
        a key that is equal to or greater than the provided stop value.

        For example, if a table included the following key/value pairs:

        b'a'=0x01, b'b'=0x02, b'd'=0x04, b'e'=0x05

        raw_items(start=b'c', stop=b'e') would only yield one tuple: (b'd', 0x04)
        """
        raise NotImplemented("must override")

    def values(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        """
        Get an iterator that yields up values from the table.
        Values will be yielded in the order of their keys after serialization
        (but the keys themselves will not be yielded).

        If start was provided, the iterator will start on the value for the
        first key that is equal to or greater than the provided start value.

        If stop was provided, the iterator will stop before yielding
        the value for the first key that is equal to or greater than the
        provided stop value.

        For example, if a table included the following key/value pairs:

        a=1, b=2, d=4, e=5

        Then values(start='c', stop='e') would only yield one value: 4
        """
        raise NotImplemented("must override")
    def raw_values(self, bytes_start: Optional[bytes]=None, bytes_stop: Optional[bytes]=None) -> Iterator:
        """
        Get an iterator that yields up actual byte values from the table.
        Values will be yielded in the order of their byte keys
        (but the keys themselves will not be yielded).
        No deserialization or decompression of values will be performed.

        If start was provided, the iterator will start on the value for the
        first key that is equal to or greater than the provided start value.

        If stop was provided, the iterator will stop before yielding
        the value for the first key that is equal to or greater than the
        provided stop value.

        For example, if a table included the following key/value pairs:

        b'a'=0x01, b'b'=0x02, b'd'=0x04, b'e'=0x05

        Then raw_values(start=b'c', stop=b'e') would only yield one value: 0x04
        """
        raise NotImplemented("must override")

class YunaTable(YunaTableBase):
    """
    This class implements a table for Yuna.

    Provides method functions for delete, get, put, etc.
    """
    def __init__(self,
        shared: YunaSharedData,
        name: str,
        key_serialize: Optional[str],
        serialize: Optional[str],
        compress: Optional[str],
    ):
        if name in shared.tables_map:
            raise ValueError(f"table '{name}' is already open in this database")

        # TODO: check key_serialize to see if we are doing integer keys here
        try:
            temp = plugins.get_serialize_plugins(key_serialize)
        except ValueError:
            raise ValueError("unknown serialization format for key_serialize: {repr(key_serialize)}")
        try:
            temp = plugins.get_serialize_plugins(serialize)
        except ValueError:
            raise ValueError("unknown serialization format for serialize: {repr(serialize)}")
        try:
            temp = plugins.get_compress_plugins(compress)
        except ValueError:
            raise ValueError("unknown compression format for compress: {repr(compress)}")

        meta = YunaTableMetadata(
            name=name,
            key_serialize=key_serialize, serialize=serialize, compress=compress
        )
        self._shared = shared

        # Check to see if the table name is in the metadata.  If it is in there, assume the table exists
        # in the LMDB file, so we wouldn't want to create.  If it's not in there, we need to create it.
        create = name not in self._shared.metadata["tables"]

        # integerkey forced false for now
        self.lmdb_table = _lmdb_table_open(self._shared.env, name, create=create, integerkey=False)

        self.name = name
        self.meta = meta

        key_serialize = meta.key_serialize

        # add method functions based on what's documented in the metadata
        env = self._shared.env
        lmdb_table = self.lmdb_table

        temp = plugins.delete_factory(env, lmdb_table, key_serialize)
        self.delete = types.MethodType(temp, self)
        temp = plugins.delete_factory(env, lmdb_table, None)
        temp.__name__ = "raw_delete"
        self.raw_delete = types.MethodType(temp, self)

        temp = plugins.get_factory(env, lmdb_table, key_serialize, serialize, compress)
        self.get = types.MethodType(temp, self)
        temp = plugins.get_factory(env, lmdb_table, None, None, None)
        temp.__name__ = "raw_get"
        self.raw_get = types.MethodType(temp, self)

        temp = plugins.put_factory(env, lmdb_table, key_serialize, serialize, compress)
        self.put = types.MethodType(temp, self)
        temp = plugins.put_factory(env, lmdb_table, None, None, None)
        temp.__name__ = "raw_put"
        self.raw_put = types.MethodType(temp, self)

        temp = plugins.items_factory(env, lmdb_table, key_serialize, serialize, compress)
        self.items = types.MethodType(temp, self)
        temp = plugins.items_factory(env, lmdb_table, None, None, None)
        temp.__name__ = "raw_items"
        self.raw_items = types.MethodType(temp, self)

        temp = plugins.keys_factory(env, lmdb_table, key_serialize)
        self.keys = types.MethodType(temp, self)
        temp = plugins.keys_factory(env, lmdb_table, None)
        temp.__name__ = "raw_keys"
        self.raw_keys = types.MethodType(temp, self)

        temp = plugins.values_factory(env, lmdb_table, key_serialize, serialize, compress)
        self.values = types.MethodType(temp, self)
        temp = plugins.values_factory(env, lmdb_table, None, None, None)
        temp.__name__ = "raw_values"
        self.raw_values = types.MethodType(temp, self)

        # Table instance fully created so keep track of it
        self._shared.tables_map[name] = self
        if create:
            self._shared.metadata["tables"][name] = vars(self.meta)
            _yuna_put_meta(self._shared.env, self._shared.metadata)
        else:
            assert self._shared.metadata["tables"][name] == vars(self.meta)

    def drop(self):
        """
        Drop a table.  Delete all key/value pairs and the table itself.
        """
        if self._shared.read_only:
            raise RuntimeError("database was opened read-only; cannot drop table")

        self._shared.is_dirty = True
        _lmdb_table_drop(self._shared.env, self.lmdb_table)
        del self._shared.tables_map[self.name]
        del self._shared.metadata["tables"][self.name]
        self._shared = self.lmdb_table = self.name = self.meta = None

    def truncate(self):
        """
        Delete all key/value pairs from table.
        """
        _lmdb_table_truncate(self._shared.env, self.lmdb_table)


class Yuna:
    """
    Key/value store with dict-like semantics.  A wrapper around LMDB.
    """
    def __init__(self,
        fname: str,
        # YunaDB name and version
        name: Optional[str]=None,
        version: Optional[int]=None,

        # details of an LMDB file follow
        read_only: bool=True,
        create: bool=False,
        safety_mode: str='a',
        single_file: bool=True,
        max_tables: int=YUNA_DEFAULT_MAX_TABLES,
        max_db_file_size: int=YUNA_DEFAULT_MAX_DB_SIZE,
        **kwargs
    ):
        if create:
            read_only = False

        env = _lmdb_open(fname,
                read_only=read_only, create=create, safety_mode=safety_mode, single_file=single_file,
                max_tables=max_tables, max_db_file_size=max_db_file_size,
                extra_args=kwargs)
        if create:
            metadata = _yuna_new_meta(name=name, version=version)
        else:
            metadata = _yuna_get_meta(env, name=name, version=version)

        tables = YunaTablesMap()
        reserved = YunaReservedTable(env=env)

        self.pathname = os.path.abspath(fname)

        self.metadata = metadata
        self.reserved = reserved
        self._shared = YunaSharedData(env=env, tables_map=vars(tables), metadata=metadata, read_only=read_only)
        self.tables = tables

        # Set up an entry in .tables for each table listed in metadata, with delete/get/put functions ready to use.
        for meta in metadata["tables"].values():
            YunaTable(self._shared, meta["name"], meta["key_serialize"], meta["serialize"], meta["compress"])

    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        # if .close() was already called we have no work to do
        if "_shared" in vars(self):
            self.close()
        return False # if there was an exception, do not suppress it

    def sync(self):
        """
        Ensure that all data is flushed to disk.

        Useful when Yuna was opened in "unsafe" mode.
        """
        if self._shared.is_dirty and not self._shared.read_only:
            _yuna_put_meta(self._shared.env, self._shared.metadata)
            self._shared.is_dirty = False
        _lmdb_sync(self._shared.env)

    def close(self):
        """
        Close the Yuna instance.

        Ensures that all data is flushed to disk.
        """
        if "_shared" not in vars(self):
            return
        if self._shared.is_dirty and not self._shared.read_only:
            _yuna_put_meta(self._shared.env, self._shared.metadata)
            self._shared.is_dirty = False
        self._shared.env.close()
        del self.tables
        del self._shared


    @property
    def table_names(self):
        return sorted(vars(self.tables))

    def new_table(self,
        name: str,
        key_serialize: Optional[str]=SERIALIZE_STR,
        serialize: Optional[str]=None,
        compress: Optional[str]=None
    ):
        """
        Open a new Yuna table.

        Creates the table in the LMDB file, updates the Yuna metadata,
        and sets up serialization and optional compression as requested.
        """
        if self._shared.read_only:
            raise RuntimeError("database was opened read-only; cannot make new table")

        tbl = YunaTable(self._shared, name, key_serialize, serialize, compress)

        # YunaTable takes care of adding the new table to self.tables
        assert name in vars(self.tables)
        # YunaTable also updates the metadata
        assert name in self._shared.metadata["tables"]

        self._shared.is_dirty = True
        return tbl

    def new_table_like(self,
        tbl: YunaTable,
        name: Optional[str],
    ):
        """
        Open a new Yuna table that's like another table that's already open.

        Looks at the metadata in the table to find how the already-open table
        was set up, then calls .new_table() with the same settings to make
        a new table set up exactly the same as the already-open table.

        If name is given as None, the new table will be given the same name
        as the already-open table.  This only makes sense if the new table is
        in a different Yuna database file than the already-open table.
        """
        # just copy all the metadata
        meta = vars(tbl.meta).copy()

        # if we have a new table name, set it in now
        if name is not None:
            meta["name"] = name

        return self.new_table(**meta)

    def repack(self):
        """
        Repack the database file to be minimal size.

        Can only be done after you call the .close() method, to make
        sure that all the data is safely written and the database
        is in a clean state.

        This actually makes a copy of the database file, then deletes
        the original file and renames the copy to the original filename.
        """
        # first, check to see if this instance was properly closed
        if "_shared" in vars(self):
            raise RuntimeError("must call call .close() before calling .repack()")

        # Use LMDB copy operation with compact=True for most efficient repacking
        pathname_repack = self.pathname + ".yuna_repack"

        # If someone interrupted an attempt to repack, clean up old repack attempt file now.
        delete_file_or_dir(pathname_repack)

        with YunaReadOnly(self.pathname, None, None) as db_old:
            db_old._shared.env.copy(pathname_repack, compact=True)

        # If no exception was raised, we have a new compacted database file!  Rename old to new.
        delete_file_or_dir(self.pathname)
        # If there's a lockfile, just delete it along with original file.
        temp = self.pathname + "-lock"
        if os.path.exists(temp):
            os.remove(temp)

        os.rename(pathname_repack, self.pathname)


class YunaReadOnly(Yuna):
    def __init__(self, *args, **kwargs):
        kwargs["read_only"] = True
        kwargs["create"] = False
        kwargs["safety_mode"] = 'u'
        super().__init__(*args, **kwargs)
