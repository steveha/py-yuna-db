# -*- coding: utf-8 -*-

"""
Yuna allows different tables to be serialized using different
serialization formats.  For example, if your needs are simple you might serialize using
JSON; for more space efficiency you might serialize using MesgPack.

Yuna allows different tables to be compressed.  You might skip this,
or use zlib or LZ4 compression, or maybe even zstandard compression.
In service of the above, this file has tables used to look up serialization
or compression.

The reason we look up live, rather than importing everything, is to allow
users to only "pip install" the optional features they need.  If you
never need to write or open a Yuna file with zstandard compression, you
you can skip "pip install zstandard".

When you attempt to open a file, the metadata lists out all the serialization
and compression formats used in the file, using tags (simple strings; for
example, the tag for LZ4 compression is the string "lz4").

If you have an old version of Yuna and you try to open a new Yuna DB file
you may have a lookup failure because a serialization or compression format
is not present in this file.  In that case, try checking for a newer
release of Yuna DB.
"""

from json import dumps as json_dumps
from json import loads as json_loads

from types import SimpleNamespace
from typing import Optional


import lmdb


from .lmdb_util import YUNA_DEFAULT_MAX_DB_SIZE, YUNA_DEFAULT_MAX_TABLES
from .lmdb_util import YUNA_DB_META_KEY, YUNA_FILE_EXTENSION

from .lmdb_util import _lmdb_open, _yuna_get_meta, _yuna_put_meta
from .lmdb_util import _lmdb_table_delete, _lmdb_table_get, _lmdb_table_put


# Create a unique object used to detect if optional arg not provided.
# Can't use None because user might want to provide None.
_YUNA_NOT_PROVIDED = object()


YUNA_SERIALIZE_CACHE = {}


class NoInit(object):
    def __init__(self, verb: str, tag: str):
        self.mesg = f"'{tag}' {verb} format does not have an init step!"
    def __call__(self, *args, **kwargs):
        raise RuntimeError(self.mesg)

def _no_init_serialize(tag: str):
    return NoInit("serialization", tag)
def _no_init_compress(tag: str):
    return NoInit("compression", tag)


class NoOptions(object):
    def __init__(self, verb: str, tag: str):
        self.mesg = f"'{tag}' {verb} format does not have any options"
    def __call__(self, *args, **kwargs):
        raise RuntimeError(self.mesg)

def _no_options_serialize(tag: str):
    return NoOptions("serialization", tag)
def _no_options_compress(tag: str):
    return NoOptions("compression", tag)


INTEGER_KEY = "integer_key"


SERIALIZE_JSON = "json"

def serialize_json(x: object):
    return json_dumps(x).encode("utf-8")

def _import_json():
    import json

    d = {
        "init": _no_init_serialize(SERIALIZE_JSON),
        "options": _no_options_serialize(SERIALIZE_JSON),
        "serialize": serialize_json,
        "deserialize": json_loads,
    }
    YUNA_SERIALIZE_CACHE[SERIALIZE_JSON] = SimpleNamespace(**d)


SERIALIZE_MSGPACK = "msgpack"


def _import_msgpack():
    import msgpack

    d = {
        "init": _no_init_serialize(SERIALIZE_MSGPACK),
        "options": _no_options_serialize(SERIALIZE_MSGPACK),
        "serialize": msgpack.dumps,
        "deserialize": msgpack.loads,
    }
    YUNA_SERIALIZE_CACHE[SERIALIZE_MSGPACK] = SimpleNamespace(**d)


SERIALIZE_STR = "str"

def _bytes_from_str(s: str):
    return s.encode('utf-8')

def _str_from_bytes(bytes_s: bytes):
    return str(bytes_s, 'utf-8')

def _import_str():
    # nothing to import; strings are built-in
    d = {
        "init": _no_init_serialize(SERIALIZE_STR),
        "options": _no_options_serialize(SERIALIZE_STR),
        "serialize": _bytes_from_str,
        "deserialize": _str_from_bytes,
    }
    YUNA_SERIALIZE_CACHE[SERIALIZE_STR] = SimpleNamespace(**d)


_SERIALIZE_IMPORT_FUNCTIONS = {
    SERIALIZE_JSON: _import_json,
    SERIALIZE_MSGPACK: _import_msgpack,
    SERIALIZE_STR: _import_str,
}



YUNA_COMPRESS_CACHE = {}

COMPRESS_LZ4 = "lz4"

def _import_lz4():
    import lz4

    d = {
        "init": _no_init_compress(COMPRESS_LZ4),
        "options": _no_options_compress(COMPRESS_LZ4),
        "compress": lz4.block.compress,
        "decompress": lz4.block.decompress,
    }
    YUNA_COMPRESS_CACHE[COMPRESS_LZ4] = SimpleNamespace(**d)


COMPRESS_ZLIB = "zlib"

def _import_zlib():
    import zlib

    d = {
        "init": _no_init_compress(COMPRESS_ZLIB),
        "options": _no_options_compress(COMPRESS_ZLIB),  # TODO: add an options function
        "compress": lz4.block.compress,
        "decompress": lz4.block.decompress,
    }
    YUNA_COMPRESS_CACHE[COMPRESS_ZLIB] = SimpleNamespace(**d)


COMPRESS_ZSTD = "zstd"

def _init_zstd():
    raise RuntimeError("init not implemented yet but is coming")
def _options_zstd(*args, **kwargs):
    raise RuntimeError("options not implemented yet but is coming")
def _train_zstd_factory():
    # build the function inside this factory so that it will close over the module reference
    def _train_zstd(size, samples):
        """
        @size: how many bytes the dictionary should be
        @samples: list of training data records

        Builds a compression dictionary of size @size from data in @samples
        """
        # TODO: see if an iterator works for @samples and update docs if it does
        dict_data = zstandard.train_dictionary(*args, **kwargs)
        bytes_data = dict_data.as_bytes()
        return bytes_data
    return _train_zstd

def _import_zstd():
    import zstandard

    d = {
        "init": _no_init_compress(COMPRESS_ZSTD), # TODO: add init()
        "options": _options_zstd,
        "compress": lz4.block.compress,
        "decompress": lz4.block.decompress,
        "train": _train_zstd_factory(),
    }
    YUNA_COMPRESS_CACHE[COMPRESS_ZSTD] = SimpleNamespace(**d)


_COMPRESS_IMPORT_FUNCTIONS = {
    COMPRESS_LZ4: _import_lz4,
    COMPRESS_ZLIB: _import_zlib,
    COMPRESS_ZSTD: _import_zstd,
}


def get_serialize_plugins(tag: Optional[str]):
    if tag is None:
        return None
    plugins = YUNA_SERIALIZE_CACHE.get(tag, None)
    if plugins is None:
        fn_import = _SERIALIZE_IMPORT_FUNCTIONS.get(tag, None)
        if fn_import is None:
            raise ValueError(f"'{tag}': unknown serialization format")
        fn_import()
    plugins = YUNA_SERIALIZE_CACHE.get(tag, None)
    if plugins is None:
        # This error should be impossible...
        # If it happens, check the import function and make sure it saves to the cache with the correct tag.
        raise RuntimeError(f"'{tag}': serialize import succeeded but cannot get plugins")
    return plugins

def get_compress_plugins(tag: str):
    if tag is None:
        return None
    plugins = YUNA_COMPRESS_CACHE.get(tag, None)
    if plugins is None:
        fn_import = _COMPRESS_IMPORT_FUNCTIONS.get(tag, None)
        if fn_import is None:
            raise ValueError(f"'{tag}': unknown compression format")
        fn_import()
    plugins = YUNA_COMPRESS_CACHE.get(tag, None)
    if plugins is None:
        # This error should be impossible...
        # If it happens, check the import function and make sure it saves to the cache with the correct tag.
        raise RuntimeError(f"'{tag}': compress import succeeded but cannot get plugins")
    return plugins


def _get_table_raw_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: str
) -> object:  # TODO put correct typing class for "function object"
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize

    def get_raw(self, key: str, default: Optional[bytes]=_YUNA_NOT_PROVIDED) -> Optional[bytes]:
        bytes_key = fn_key_serialize(key)
        result = _lmdb_table_get(env, table, bytes_key, None)
        if result is None:
            if default is _YUNA_NOT_PROVIDED:
                raise KeyError(key)
            else:
                return default
        return result
    return get_raw

def _get_table_deserialize_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: str,
    value_serialize_tag: str
) -> object:  # TODO put correct typing class for "function object"
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize

    value_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_deserialize = value_plugins.deserialize

    def get(self, key: str, default: object=_YUNA_NOT_PROVIDED) -> object:
        bytes_key = fn_key_serialize(key)
        result = _lmdb_table_get(env, table, bytes_key, None)
        if result is None:
            if default is _YUNA_NOT_PROVIDED:
                raise KeyError(key)
            else:
                return default
        return fn_value_deserialize(result)
    return get

def _get_table_decompress_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: str,
    value_compress_tag: str
) -> object:  # TODO put correct typing class for "function object"
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize

    value_plugins = get_compress_plugins(value_compress_tag)
    fn_value_decompress = value_plugins.decompress

    def get(self, key: str, default: object=_YUNA_NOT_PROVIDED) -> object:
        bytes_key = fn_key_serialize(key)
        result = _lmdb_table_get(env, table, bytes_key, None)
        if result is None:
            if default is _YUNA_NOT_PROVIDED:
                raise KeyError(key)
            else:
                return default
        return fn_value_decompress(result)
    return get

def _get_table_deserialize_decompress_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: str,
    value_serialize_tag: str,
    value_compress_tag: str
) -> object:  # TODO put correct typing class for "function object"
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize

    value_serialize_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_deserialize = value_serialize_plugins.deserialize

    value_compress_plugins = get_compress_plugins(value_compress_tag)
    fn_value_decompress = value_compress_plugins.deserialize

    def get(self, key: str, default: object=_YUNA_NOT_PROVIDED) -> object:
        bytes_key = fn_key_serialize(key)
        result = _lmdb_table_get(env, table, bytes_key, None)
        if result is None:
            if default is _YUNA_NOT_PROVIDED:
                raise KeyError(key)
            else:
                return default
        return fn_value_deserialize(fn_value_decompress(result))
    return get

def get_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: str,
    value_serialize_tag: Optional[str],
    value_compress_tag: Optional[str]
) -> object:  # TODO put correct typing class for "function object"
    if value_serialize_tag and value_compress_tag:
        return _get_table_deserialize_decompress_factory(
                env, table, key_serialize_tag, value_serialize_tag, value_compress_tag)
    elif not value_serialize_tag and value_compress_tag:
        return _get_table_decompress_factory(
                env, table, key_serialize_tag, value_compress_tag)
    elif value_serialize_tag and not value_compress_tag:
        return _get_table_deserialize_factory(
                env, table, key_serialize_tag, value_serialize_tag)
    else:
        return _get_table_raw_factory(env, table, key_serialize_tag)


def _put_table_raw_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: str
) -> object:  # TODO put correct typing class for "function object"
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize

    def put_raw(self, key: str, bytes_value: bytes) -> None:
        bytes_key = fn_key_serialize(key)
        _lmdb_table_put(env, table, bytes_key, bytes_value)
    return put_raw

def _put_table_serialize_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: str,
    value_serialize_tag: str
) -> object:  # TODO put correct typing class for "function object"
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize

    value_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_serialize = value_plugins.serialize

    def put(self, key: str, value: object) -> object:
        bytes_key = fn_key_serialize(key)
        bytes_value = fn_value_serialize(value)
        _lmdb_table_put(env, table, bytes_key, bytes_value)
    return put

def _put_table_compress_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: str,
    value_compress_tag: str
) -> object:  # TODO put correct typing class for "function object"
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize

    value_plugins = get_compress_plugins(value_compress_tag)
    fn_value_compress = value_plugins.compress

    def put(self, key: str, value: object) -> object:
        bytes_key = fn_key_serialize(key)
        bytes_value = fn_value_compress(result)
        _lmdb_table_put(env, table, bytes_key, bytes_value)
    return put

def _put_table_serialize_compress_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: str,
    value_serialize_tag: str,
    value_compress_tag: str
) -> object:  # TODO put correct typing class for "function object"
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize

    value_serialize_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_serialize = value_serialize_plugins.serialize

    value_compress_plugins = get_compress_plugins(value_compress_tag)
    fn_value_compress = value_compress_plugins.serialize

    def put(self, key: str, value: object) -> object:
        bytes_key = fn_key_serialize(key)
        bytes_value = fn_value_compress(fn_value_serialize(value))
        _lmdb_table_put(env, table, bytes_key, bytes_value)
    return put

def put_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: str,
    value_serialize_tag: Optional[str],
    value_compress_tag: Optional[str]
) -> object:  # TODO put correct typing class for "function object"
    if value_serialize_tag and value_compress_tag:
        return _put_table_serialize_compress_factory(
                env, table, key_serialize_tag, value_serialize_tag, value_compress_tag)
    elif not value_serialize_tag and value_compress_tag:
        return _put_table_compress_factory(
                env, table, key_serialize_tag, value_compress_tag)
    elif value_serialize_tag and not value_compress_tag:
        return _put_table_serialize_factory(
                env, table, key_serialize_tag, value_serialize_tag)
    else:
        return _put_table_raw_factory(env, table, key_serialize_tag)

def delete_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: str
) -> object:  # TODO put correct typing class for "function object"
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize

    def delete(self, key: str) -> None:
        bytes_key = fn_key_serialize(key)
        _lmdb_table_delete(env, table, bytes_key)
    return delete
