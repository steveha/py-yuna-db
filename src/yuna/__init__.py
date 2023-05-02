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

__version__ = "0.2.2"

import os
import types

from typing import Any, Iterator, Optional

import lmdb


from .lmdb_util import YUNA_DEFAULT_MAX_DB_SIZE, YUNA_DEFAULT_MAX_TABLES
from .lmdb_util import YUNA_DB_META_KEY, YUNA_FILE_EXTENSION

from .lmdb_util import _lmdb_open, _yuna_get_meta, _yuna_new_meta, _yuna_put_meta
from .lmdb_util import _lmdb_reserved_delete, _lmdb_reserved_get, _lmdb_reserved_put
from .lmdb_util import _lmdb_table_drop, _lmdb_table_open, _lmdb_table_truncate

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
    def __init__(self, env: lmdb.Environment, tables_map: dict, metadata: dict):
        self.env = env
        self.tables_map = tables_map
        self.metadata = metadata


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
    pass


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


class YunaTable:
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
        _lmdb_table_drop(self._shared.env, self.lmdb_table)
        del self._shared.tables_map[self.name]
        del self._shared.metadata["tables"][self.name]
        self._shared = self.lmdb_table = self.name = self.meta = None

    def truncate(self):
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
        self._shared = YunaSharedData(env=env, tables_map=vars(tables), metadata=metadata)
        self.tables = tables

        # Set up an entry in .tables for each table listed in metadata, with delete/get/put functions ready to use.
        for meta in metadata["tables"].values():
            YunaTable(self._shared, meta["name"], meta["key_serialize"], meta["serialize"], meta["compress"])

    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self._shared.env.close()
        return False # if there was an exception, do not suppress it

    def sync(self):
        """
        Ensure that all data is flushed to disk.

        Useful when Yuna was opened in "unsafe" mode.
        """
        _lmdb_sync(self._shared.env)
    def close(self):
        """
        Close the Yuna instance.

        Ensures that all data is flushed to disk.
        """
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
        tbl = YunaTable(self._shared, name, key_serialize, serialize, compress)

        # YunaTable takes care of adding the new table to self.tables
        assert name in vars(self.tables)
        # YunaTable also updates the metadata
        assert name in self._shared.metadata["tables"]

        return tbl

class YunaReadOnly(Yuna):
    def __init__(self, *args, **kwargs):
        kwargs["read_only"] = True
        kwargs["create"] = False
        kwargs["safety_mode"] = 'u'
        super().__init__(*args, **kwargs)
