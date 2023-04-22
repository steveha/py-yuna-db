# -*- coding: utf-8 -*-

"""
lmdb_util.py -- LMDB low-level convenience functions

Functions to handle common LMDB operations.

"""


import json
import os
import shutil

from typing import Optional


import lmdb


# Create a unique object used to detect if optional arg not provided.
# Can't use None because user might want to provide None.
_LMDB_UTIL_NOT_PROVIDED = object()


# NOTES about terminology
#
# The LMDB documentation uses some unusual terms.  The Yuna library uses some
# different terms.
#
# An LMDB file may contain multiple key/value stores.  LMDB refers to these
# as "databases" or "named databases".  Yuna will simply call these "tables".
#
# Every LMDB file has one, default key/value store that the user can store
# data in, but that LMDB also stores some data in.  LMDB calls this
# the "unnamed database".  Yuna calls this the "reserved table".
#
# LMDB calls the open LMDB file an "environment" and uses "env" as
# the variable name for what is returned.  Yuna just calls it "the database"
# and uses "lmdb" to refer to the open LMDB instance.

# LMDB files have to be declared with limits: maximum number of tables,
# maximum total file size.  In practice it works quite well to simply give
# very large limit numbers; the database will not actually take up the
# maximum declared size.  On Linux, the file may appear to be that size,
# but in that case the "yuna_repack" utility can be used to make a copy
# of the database file that is minimum size.


class YunaInvalidDB(ValueError):
    pass

YUNA_FILE_EXTENSION = ".ydb"

YUNA_DB_META_KEY = "__YUNA_DB_META__"

YUNA_DEFAULT_MAX_TABLES = 100
YUNA_DEFAULT_MAX_DB_SIZE = 2**40  # one tebibyte (binary terabyte): 1024**4

_VALID_SAFETY_MODES = ('a', 'u')


def _delete_file_or_dir(fname: str):
    # If it doesn't exist, we don't complain, similar to /bin/rm -f on Linux
    if not os.path.exists(fname):
        return

    # Something exists; delete it whether it is a file or a directory.
    try:
        os.remove(fname)
    except IsADirectoryError:
        shutil.rmtree(fname)

def _lmdb_reserved_delete(
    env: lmdb.Environment,
    bytes_key: bytes
) -> None:
    with env.begin(write=True) as txn:
        txn.delete(bytes_key)

def _lmdb_reserved_get(
    env: lmdb.Environment,
    bytes_key: bytes,
    default: Optional[bytes]=_LMDB_UTIL_NOT_PROVIDED
) -> None:
    with env.begin() as txn:
        result = txn.get(bytes_key, default=None)

    if result is None:
        if default is _LMDB_UTIL_NOT_PROVIDED:
            raise KeyError(key)
        else:
            return default
    return result

def _lmdb_reserved_put(
    env: lmdb.Environment,
    bytes_key: bytes,
    bytes_value: bytes
) -> None:
    with env.begin(write=True) as txn:
        txn.put(bytes_key, bytes_value)


def _lmdb_table_open(
    env: lmdb.Environment,
    name: str,
    create: bool=False,
    integerkey: bool=False
) -> None:
    bytes_name = bytes(name, "utf-8")
    return env.open_db(bytes_name, create=create, integerkey=integerkey)


def _lmdb_table_delete(
    env: lmdb.Environment,
    table: lmdb._Database,
    bytes_key: bytes
) -> None:
    with env.begin(db=table, write=True) as txn:
        txn.delete(bytes_key)

def _lmdb_table_drop(
    env: lmdb.Environment,
    table: lmdb._Database
) -> None:
    # With delete=True drops the entire table and all data contained inside it.
    with env.begin(write=True) as txn:
        txn.drop(table, delete=True)

def _lmdb_table_get(
    env: lmdb.Environment,
    table: lmdb._Database,
    key: bytes,
    default: Optional[bytes]=_LMDB_UTIL_NOT_PROVIDED,
) -> Optional[bytes]:
    with env.begin(db=table) as txn:
        result = txn.get(key, default=None)

    if result is None:
        if default is _LMDB_UTIL_NOT_PROVIDED:
            raise KeyError(key)
        else:
            return default
    return result

def _lmdb_table_put(
    env: lmdb.Environment,
    table: lmdb._Database,
    key: bytes,
    value: bytes,
) -> None:
    with env.begin(db=table, write=True) as txn:
        txn.put(key, value)

def _lmdb_table_truncate(
    env: lmdb.Environment,
    table: lmdb._Database
) -> None:
    with env.begin(write=True) as txn:
        # Drops every key/value pair, but with delete=False does not drop the table itself.
        txn.drop(table, delete=False)

def _lmdb_sync(
    env: lmdb.Environment
) -> None:
    env.sync(force=True)



def _yuna_new_meta(
    name: Optional[str]=None,
    version: Optional[str]=None,
    tables_map: Optional[dict]=None,
) -> dict:
    if tables_map is None:
        tables_map = {}

    metadata = {}
    if name is not None:
        metadata["name"] = name
    if version is not None:
        metadata["version"] = version
    metadata["tables"] = {}

    metadata["yuna_version"] = 1

    return metadata

def _yuna_get_meta(
    env: lmdb.Environment,
    name: Optional[str],
    version: Optional[int],
) -> dict:
    """
    Get the metadata stored in a Yuna DB file.

    Given an open LMDB file, first check to see if it's a valid
    Yuna DB file.  If it is, return the metadata as a dict.

    If @name is provided, check to see if the Yuna DB file has that name.
    If @version is provided, check to see if the Yuna DB file has that version.

    Raises YunaInvalidDB on any error.
    """
    # Try to retrieve the metadata, always stored in the reserved table.
    key = bytes(YUNA_DB_META_KEY, "utf-8")
    value = _lmdb_reserved_get(env, key, default=None)
    if value is None:
        fname = env.path()
        raise YunaInvalidDB(f"LMDB file is not a Yuna DB file: {repr(fname)}")

    # We got something... does it decode as valid JSON?
    try:
        meta = json.loads(value)
        if not isinstance(meta, dict):
            # whatever was stored there wasn't a JSON-encoded metadata dictionary
            raise ValueError
    except (ValueError, TypeError, json.decoder.JSONDecodeError):
        fname = env.path()
        raise YunaInvalidDB(f"Yuna DB has corrupted metadata: {repr(fname)}")

    # If user provided name and/or version, make appropriate checks.
    if name is not None:
        temp = meta.get("name", None)
        if temp != name:
            raise YunaInvalidDB(f"LMDB file name mismatch: expected {repr(name)}, got {repr(temp)}")
    if version is not None:
        temp = meta.get("version", None)
        if temp != version:
            raise YunaInvalidDB(f"LMDB file version mismatch: expected {version}, got {temp}")
    return meta


def _yuna_put_meta(
    env: lmdb.Environment,
    meta: dict,
) -> None:
    """
    Given @meta, a dictionary containing metadata, write it to the LMDB file.
    """
    # Try to retrieve the metadata, always stored in the reserved table.
    key = bytes(YUNA_DB_META_KEY, "ascii")
    s = json.dumps(meta)
    value = bytes(s, "utf-8")
    _lmdb_reserved_put(env, key, value)


def _lmdb_open(
    fname: str,
    read_only: bool=True,
    create: bool=False,
    safety_mode: str='a',
    single_file: bool=True,
    max_tables: int=YUNA_DEFAULT_MAX_TABLES,
    max_db_file_size: int=YUNA_DEFAULT_MAX_DB_SIZE,
    extra_args: Optional[dict]=None,
):
    """
    @safety_mode: legal values are 'a' (ACID safety)  'u' (unsafe; fastest)
    """
    if safety_mode not in _VALID_SAFETY_MODES:
        mesg = f"safety_mode must be one of {_VALID_SAFETY_MODES} "\
            "but instead was {repr(safety_mode)}"
        raise ValueError(mesg)

    if not create:
        if not os.path.exists(fname) and not fname.endswith(YUNA_FILE_EXTENSION):
            temp = fname + YUNA_FILE_EXTENSION
            if os.path.exists(fname):
                fname = temp

        if not os.path.exists(fname):
            raise FileNotFoundError(fname)

    # Create implies we want to be able to write.  Don't even check it, just make sure read_only is False.
    if create:
        read_only = False
        _delete_file_or_dir(fname)

    try:
        kwargs = {
            "create": create,
            "map_size": max_db_file_size,
            "max_dbs": max_tables,
            "readonly": read_only,
            "subdir": not single_file,
        }

        if safety_mode == 'u':
            # Change all the settings to their fastest and least safe value.
            # This is ideal for creating a DB file that will later be used
            # read-only, and is a terrible idea for a DB that will be "live"
            # and will have data written to it when a service is running.
            kwargs["metasync"] = False
            kwargs["sync"] = False
            kwargs["writemap"] = True
            kwargs["map_async"] = True

        if extra_args:
            kwargs.update(extra_args)
        env = lmdb.open(fname, **kwargs)
        return env
    except Exception:
        # currently Yuna is just letting LMDB exceptions be raised.
        raise
