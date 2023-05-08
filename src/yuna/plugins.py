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

from typing import Any, Callable, Dict, Iterator, List, Optional


import lmdb


from .lmdb_util import YUNA_DEFAULT_MAX_DB_SIZE, YUNA_DEFAULT_MAX_TABLES
from .lmdb_util import YUNA_DB_META_KEY, YUNA_FILE_EXTENSION

from .lmdb_util import _lmdb_open, _yuna_get_meta, _yuna_put_meta
from .lmdb_util import _lmdb_table_delete, _lmdb_table_get, _lmdb_table_put


# Create a unique object used to detect if optional arg not provided.
# Can't use None because user might want to provide None.
_YUNA_NOT_PROVIDED = object()


class SerializePlugins:
    def __init__(self,
        serialize: Callable,
        deserialize: Callable,
        init: Optional[Callable] = None,
        options: Optional[Callable] = None,
    ):
        self.serialize = serialize
        self.deserialize = deserialize
        self.init = init
        self.options = options

class CompressPlugins:
    def __init__(self,
        compress: Callable,
        decompress: Callable,
        init: Optional[Callable] = None,
        options: Optional[Callable] = None,
        train: Optional[Callable] = None,
    ):
        self.compress = compress
        self.decompress = decompress
        self.init = init
        self.options = options
        self.train = train


YUNA_SERIALIZE_CACHE: Dict[str, SerializePlugins] = {}


def _not_implemented(*args, **kwargs) -> None:
    raise NotImplemented("not implemented yet")

def _empty_string_key_check(x: str) -> None:
    # LMDB doesn't allow zero-length keys.  For now, raise when one is seen.  TODO: consider adding a workaround.
    if not isinstance(x, int) and x is not None:
        if not x:
            raise ValueError("key cannot be empty string")


INTEGER_KEY = "integer_key"


SERIALIZE_JSON = "json"

def serialize_json(x: Any) -> bytes:
    return json_dumps(x).encode("utf-8")

deserialize_json = json_loads

def _import_json() -> None:
    import json

    plugins = SerializePlugins(
        serialize=serialize_json,
        deserialize=deserialize_json,
    )
    YUNA_SERIALIZE_CACHE[SERIALIZE_JSON] = plugins


SERIALIZE_MSGPACK = "msgpack"


def _import_msgpack() -> None:
    import msgpack

    plugins = SerializePlugins(
        serialize=msgpack.dumps,
        deserialize=msgpack.loads,
    )
    YUNA_SERIALIZE_CACHE[SERIALIZE_MSGPACK] = plugins


SERIALIZE_STR = "str"

def serialize_str(s: str) -> bytes:
    return s.encode('utf-8')

def deserialize_str(bytes_s: bytes) -> str:
    return str(bytes_s, 'utf-8')

def _import_str() -> None:
    # nothing to import; strings are built-in
    plugins = SerializePlugins(
        serialize=serialize_str,
        deserialize=deserialize_str,
    )
    YUNA_SERIALIZE_CACHE[SERIALIZE_STR] = plugins


_SERIALIZE_IMPORT_FUNCTIONS = {
    SERIALIZE_JSON: _import_json,
    SERIALIZE_MSGPACK: _import_msgpack,
    SERIALIZE_STR: _import_str,
}



YUNA_COMPRESS_CACHE: Dict[str, CompressPlugins] = {}


COMPRESS_LZ4 = "lz4"

def _import_lz4() -> None:
    import lz4
    import lz4.block

    plugins = CompressPlugins(
        compress=lz4.block.compress,
        decompress=lz4.block.decompress,
    )
    YUNA_COMPRESS_CACHE[COMPRESS_LZ4] = plugins


COMPRESS_ZLIB = "zlib"

def _import_zlib() -> None:
    import zlib

    plugins = CompressPlugins(
        init=None,
        options=None,  # TODO: add an options function
        compress=zlib.compress,
        decompress=zlib.decompress,
    )
    YUNA_COMPRESS_CACHE[COMPRESS_ZLIB] = plugins


COMPRESS_ZSTD = "zstd"

def _init_zstd():
    raise RuntimeError("init not implemented yet but is coming")
def _options_zstd(*args, **kwargs):
    raise RuntimeError("options not implemented yet but is coming")
def _train_zstd_factory() -> Callable:
    # build the function inside this factory so that it will close over the module reference
    fn_train_dictionary = zstandard.train_dictionary
    def _train_zstd(size: int, samples: List[Any]) -> bytes:
        """
        @size: how many bytes the dictionary should be
        @samples: list of training data records

        Builds a compression dictionary of size @size from data in @samples
        """
        # TODO: see if an iterator works for @samples and update docs if it does
        compression_dictionary = fn_train_dictionary(*args, **kwargs)
        bytes_data = compression_dictionary.as_bytes()
        return bytes_data
    return _train_zstd

def _import_zstd() -> None:
    import zstandard

    plugins = CompressPlugins(
        init=None, # TODO: add init()
        options=None,  # TODO: add an options function
        compress=_not_implemented,
        decompress=_not_implemented,
        train=_train_zstd_factory(),
    )
    YUNA_COMPRESS_CACHE[COMPRESS_ZSTD] = plugins


_COMPRESS_IMPORT_FUNCTIONS = {
    COMPRESS_LZ4: _import_lz4,
    COMPRESS_ZLIB: _import_zlib,
    COMPRESS_ZSTD: _import_zstd,
}


def get_serialize_plugins(tag: Optional[str]) -> dict:
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


# Python strings are much more convenient as keys than Python byte strings.
# So, while it's legal to use byte strings as keys, Yuna doesn't really
# expect that case, so we will always have a key serialization function.
# This function does nothing, very quickly, to handle that case.  If the
# user is passing a byte string anyway we can return it unchanged.
def _return_bytes_unchanged(x: bytes) -> bytes:
    """
    Return a byte string unchanged.

    Used as a key serialization function in cases where
    no serialization is requested.
    """
    return x


def _get_table_raw_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str]
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    def get(self, key: str, default: Optional[bytes]=_YUNA_NOT_PROVIDED) -> Optional[bytes]:
        bytes_key = fn_key_serialize(key)
        result = _lmdb_table_get(env, table, bytes_key, None)
        if result is None:
            if default is _YUNA_NOT_PROVIDED:
                raise KeyError(key)
            else:
                return default
        return result
    return get

def _get_table_deserialize_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_serialize_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    value_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_deserialize = value_plugins.deserialize

    def get(self, key: str, default: Any=_YUNA_NOT_PROVIDED) -> Any:
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
    key_serialize_tag: Optional[str],
    value_compress_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    value_plugins = get_compress_plugins(value_compress_tag)
    fn_value_decompress = value_plugins.decompress

    def get(self, key: str, default: Any=_YUNA_NOT_PROVIDED) -> Any:
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
    key_serialize_tag: Optional[str],
    value_serialize_tag: str,
    value_compress_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    value_serialize_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_deserialize = value_serialize_plugins.deserialize

    value_compress_plugins = get_compress_plugins(value_compress_tag)
    fn_value_decompress = value_compress_plugins.decompress

    def get(self, key: str, default: Any=_YUNA_NOT_PROVIDED) -> Any:
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
    key_serialize_tag: Optional[str],
    value_serialize_tag: Optional[str],
    value_compress_tag: Optional[str]
) -> Callable:
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
    key_serialize_tag: Optional[str]
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    def put(self, key: str, bytes_value: bytes) -> None:
        bytes_key = fn_key_serialize(key)
        _lmdb_table_put(env, table, bytes_key, bytes_value)
    return put

def _put_table_serialize_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_serialize_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    value_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_serialize = value_plugins.serialize

    def put(self, key: str, value: Any) -> None:
        bytes_key = fn_key_serialize(key)
        bytes_value = fn_value_serialize(value)
        _lmdb_table_put(env, table, bytes_key, bytes_value)
    return put

def _put_table_compress_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_compress_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    value_plugins = get_compress_plugins(value_compress_tag)
    fn_value_compress = value_plugins.compress

    def put(self, key: str, value: Any) -> None:
        bytes_key = fn_key_serialize(key)
        bytes_value = fn_value_compress(result)
        _lmdb_table_put(env, table, bytes_key, bytes_value)
    return put

def _put_table_serialize_compress_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_serialize_tag: str,
    value_compress_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    value_serialize_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_serialize = value_serialize_plugins.serialize

    value_compress_plugins = get_compress_plugins(value_compress_tag)
    fn_value_compress = value_compress_plugins.compress

    def put(self, key: str, value: Any) -> None:
        bytes_key = fn_key_serialize(key)
        bytes_value = fn_value_compress(fn_value_serialize(value))
        _lmdb_table_put(env, table, bytes_key, bytes_value)
    return put

def put_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_serialize_tag: Optional[str],
    value_compress_tag: Optional[str]
) -> Callable:
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
    key_serialize_tag: Optional[str]
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    def delete(self, key: str) -> None:
        bytes_key = fn_key_serialize(key)
        _lmdb_table_delete(env, table, bytes_key)
    return delete


# TODO: change the types on keys from str to Any
def keys_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str]
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged
    fn_key_deserialize = key_plugins.deserialize if key_plugins else _return_bytes_unchanged

    def keys(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        _empty_string_key_check(start)
        _empty_string_key_check(stop)
        bytes_start = fn_key_serialize(start) if (start is not None) else None
        bytes_stop = fn_key_serialize(stop) if (stop is not None) else None
        with env.begin() as txn:
            with txn.cursor(table) as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    for bytes_key, _ in cursor:
                        key = fn_key_deserialize(bytes_key)
                        yield key
                else:
                    for bytes_key, _ in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        key = fn_key_deserialize(bytes_key)
                        yield key
    return keys


def _items_table_raw_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str]
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged
    fn_key_deserialize = key_plugins.deserialize if key_plugins else _return_bytes_unchanged

    def items(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        _empty_string_key_check(start)
        _empty_string_key_check(stop)
        bytes_start = fn_key_serialize(start) if (start is not None) else None
        bytes_stop = fn_key_serialize(stop) if (stop is not None) else None
        with env.begin() as txn:
            with txn.cursor(table) as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    if fn_key_deserialize is _return_bytes_unchanged:
                        # The very fastest possible case: return byte keys and byte values, just use yield from!
                        yield from cursor
                    else:
                        for bytes_key, bytes_value in cursor:
                            key = fn_key_deserialize(bytes_key)
                            yield key, bytes_value
                else:
                    for bytes_key, bytes_value in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        key = fn_key_deserialize(bytes_key)
                        yield key, bytes_value
    return items

def _items_table_deserialize_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_serialize_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged
    fn_key_deserialize = key_plugins.deserialize if key_plugins else _return_bytes_unchanged

    value_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_deserialize = value_plugins.deserialize

    def items(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        _empty_string_key_check(start)
        _empty_string_key_check(stop)
        bytes_start = fn_key_serialize(start) if (start is not None) else None
        bytes_stop = fn_key_serialize(stop) if (stop is not None) else None
        with env.begin() as txn:
            with txn.cursor(table) as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    for bytes_key, bytes_value in cursor:
                        key = fn_key_deserialize(bytes_key)
                        value = fn_value_deserialize(bytes_value)
                        yield key, value
                else:
                    for bytes_key, bytes_value in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        key = fn_key_deserialize(bytes_key)
                        value = fn_value_deserialize(bytes_value)
                        yield key, value
    return items

def _items_table_decompress_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_compress_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged
    fn_key_deserialize = key_plugins.deserialize if key_plugins else _return_bytes_unchanged

    value_plugins = get_compress_plugins(value_compress_tag)
    fn_value_decompress = value_plugins.decompress

    def items(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        _empty_string_key_check(start)
        _empty_string_key_check(stop)
        bytes_start = fn_key_serialize(start) if (start is not None) else None
        bytes_stop = fn_key_serialize(stop) if (stop is not None) else None
        with env.begin() as txn:
            with txn.cursor(table) as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    for bytes_key, bytes_value in cursor:
                        key = fn_key_deserialize(bytes_key)
                        value = fn_value_decompress(bytes_value)
                        yield key, value
                else:
                    for bytes_key, bytes_value in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        key = fn_key_deserialize(bytes_key)
                        value = fn_value_decompress(bytes_value)
                        yield key, value
    return items

def _items_table_deserialize_decompress_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_serialize_tag: str,
    value_compress_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged
    fn_key_deserialize = key_plugins.deserialize if key_plugins else _return_bytes_unchanged

    value_serialize_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_deserialize = value_serialize_plugins.deserialize

    value_compress_plugins = get_compress_plugins(value_compress_tag)
    fn_value_decompress = value_compress_plugins.decompress

    def items(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        _empty_string_key_check(start)
        _empty_string_key_check(stop)
        bytes_start = fn_key_serialize(start) if (start is not None) else None
        bytes_stop = fn_key_serialize(stop) if (stop is not None) else None
        with env.begin() as txn:
            with txn.cursor(table) as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    for bytes_key, bytes_value in cursor:
                        key = fn_key_deserialize(bytes_key)
                        value = fn_value_deserialize(fn_value_decompress(bytes_value))
                        yield key, value
                else:
                    for bytes_key, bytes_value in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        key = fn_key_deserialize(bytes_key)
                        value = fn_value_deserialize(fn_value_decompress(bytes_value))
                        yield key, value
    return items

def items_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_serialize_tag: Optional[str],
    value_compress_tag: Optional[str]
) -> Callable:
    if value_serialize_tag and value_compress_tag:
        return _items_table_deserialize_decompress_factory(
                env, table, key_serialize_tag, value_serialize_tag, value_compress_tag)
    elif not value_serialize_tag and value_compress_tag:
        return _items_table_decompress_factory(
                env, table, key_serialize_tag, value_compress_tag)
    elif value_serialize_tag and not value_compress_tag:
        return _items_table_deserialize_factory(
                env, table, key_serialize_tag, value_serialize_tag)
    else:
        return _items_table_raw_factory(env, table, key_serialize_tag)


def _values_table_raw_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str]
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    def values(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        _empty_string_key_check(start)
        _empty_string_key_check(stop)
        bytes_start = fn_key_serialize(start) if (start is not None) else None
        bytes_stop = fn_key_serialize(stop) if (stop is not None) else None
        with env.begin() as txn:
            with txn.cursor(table) as cursor:
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
    return values

def _values_table_deserialize_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_serialize_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    value_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_deserialize = value_plugins.deserialize

    def values(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        _empty_string_key_check(start)
        _empty_string_key_check(stop)
        bytes_start = fn_key_serialize(start) if (start is not None) else None
        bytes_stop = fn_key_serialize(stop) if (stop is not None) else None
        with env.begin() as txn:
            with txn.cursor(table) as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    for _, bytes_value in cursor:
                        value = fn_value_deserialize(bytes_value)
                        yield value
                else:
                    for bytes_key, bytes_value in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        value = fn_value_deserialize(bytes_value)
                        yield value
    return values

def _values_table_decompress_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_compress_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    value_plugins = get_compress_plugins(value_compress_tag)
    fn_value_decompress = value_plugins.decompress

    def values(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        _empty_string_key_check(start)
        _empty_string_key_check(stop)
        bytes_start = fn_key_serialize(start) if (start is not None) else None
        bytes_stop = fn_key_serialize(stop) if (stop is not None) else None
        with env.begin() as txn:
            with txn.cursor(table) as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    for _, bytes_value in cursor:
                        value = fn_value_decompress(bytes_value)
                        yield value
                else:
                    for bytes_key, bytes_value in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        value = fn_value_decompress(bytes_value)
                        yield value
    return values

def _values_table_deserialize_decompress_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_serialize_tag: str,
    value_compress_tag: str
) -> Callable:
    key_plugins = get_serialize_plugins(key_serialize_tag)
    fn_key_serialize = key_plugins.serialize if key_plugins else _return_bytes_unchanged

    value_serialize_plugins = get_serialize_plugins(value_serialize_tag)
    fn_value_deserialize = value_serialize_plugins.deserialize

    value_compress_plugins = get_compress_plugins(value_compress_tag)
    fn_value_decompress = value_compress_plugins.decompress

    def values(self, start: Optional[str]=None, stop: Optional[str]=None) -> Iterator:
        _empty_string_key_check(start)
        _empty_string_key_check(stop)
        bytes_start = fn_key_serialize(start) if (start is not None) else None
        bytes_stop = fn_key_serialize(stop) if (stop is not None) else None
        with env.begin() as txn:
            with txn.cursor(table) as cursor:
                if bytes_start is not None:
                    cursor.set_range(bytes_start)
                if bytes_stop is None:
                    for _, bytes_value in cursor:
                        value = fn_value_deserialize(fn_value_decompress(bytes_value))
                        yield value
                else:
                    for bytes_key, bytes_value in cursor:
                        if bytes_key >= bytes_stop:
                            break
                        value = fn_value_deserialize(fn_value_decompress(bytes_value))
                        yield value
    return values

def values_factory(
    env: lmdb.Environment,
    table: lmdb._Database,
    key_serialize_tag: Optional[str],
    value_serialize_tag: Optional[str],
    value_compress_tag: Optional[str]
) -> Callable:
    if value_serialize_tag and value_compress_tag:
        return _values_table_deserialize_decompress_factory(
                env, table, key_serialize_tag, value_serialize_tag, value_compress_tag)
    elif not value_serialize_tag and value_compress_tag:
        return _values_table_decompress_factory(
                env, table, key_serialize_tag, value_compress_tag)
    elif value_serialize_tag and not value_compress_tag:
        return _values_table_deserialize_factory(
                env, table, key_serialize_tag, value_serialize_tag)
    else:
        return _values_table_raw_factory(env, table, key_serialize_tag)
