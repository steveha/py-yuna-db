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

__version__ = "0.1.1"

import types

from json import dumps as json_dumps
from json import loads as json_loads
from types import SimpleNamespace
from typing import Optional

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
from .plugins import serialize_json


class YunaSharedData(object):
    """
    Private data for Yuna, in a class by itself so it can be shared
    among the multiple classes implementing Yuna.
    """
    def __init__(self, env: lmdb.Environment, tables_map: dict, metadata: dict):
        self.env = env
        self.tables_map = tables_map
        self.metadata = metadata


class YunaReservedTable(object):
    """
    This class provides method functions to get/put values from the
    LMDB reserved table.

    This will be .reserved in the open Yuna instance.
    """
    def __init__(self, env: lmdb.Environment):
        self.env = env

    def delete(self, key: str):
        bytes_key = bytes(key, 'utf-8')
        _lmdb_reserved_delete(self.env, bytes_key)

    def get(self, key: str, default: object=_YUNA_NOT_PROVIDED):
        bytes_key = bytes(key, 'utf-8')
        bytes_value = _lmdb_reserved_get(self.env, bytes_key, None)
        if bytes_value is None:
            if default is _YUNA_NOT_PROVIDED:
                raise KeyError(key)
            else:
                return default
        value = json_loads(bytes_value)
        return value

    def put(self, key: str, value: object):
        bytes_key = bytes(key, 'utf-8')
        bytes_value = serialize_json(value)
        _lmdb_reserved_put(self.env, bytes_key, bytes_value)


class YunaTablesMap(object):
    """
    A trvial class, just used as a container for instances of YunaTable.

    This will be .tables in the open Yuna instance.
    """
    def __init__(self):
        pass


class YunaTable(object):
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

        meta = SimpleNamespace(
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

        fn_delete = plugins.delete_factory(env, lmdb_table, key_serialize)
        fn_get = plugins.get_factory(env, lmdb_table, key_serialize, serialize, compress)
        fn_put = plugins.put_factory(env, lmdb_table, key_serialize, serialize, compress)

        # turn the freshly-created function objects into bound method functions of the class
        self.delete = types.MethodType(fn_delete, self)
        self.get = types.MethodType(fn_get, self)
        self.put = types.MethodType(fn_put, self)

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


class Yuna(object):
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

        self.metadata = metadata
        self.reserved = reserved
        self._shared = YunaSharedData(env=env, tables_map=vars(tables), metadata=metadata)
        self.tables = tables

        # Set up an entry in .tables for each table listed in metadata, with delete/get/put functions ready to use.
        for meta in metadata["tables"].values():
            YunaTable(self._shared, meta["name"], meta["key_serialize"], meta["serialize"], meta["compress"])

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
