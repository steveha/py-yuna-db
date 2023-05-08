#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
test_yuna.py -- Test Yuna DB

We need a set of unit tests, but for now we have this.

Exercise several Yuna features to make sure that at least
the basics are working.

"""

import os
import sys

# Hack sys.path so that this file will run against Yuna from this directory tree,
# even if someone ran "pip install yuna-db" before running this.
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(THIS_DIR, "src")))

import yuna
from yuna import Yuna
from yuna.lmdb_util import delete_file_or_dir

TEST_FILE = "/tmp/junk.ydb"

delete_file_or_dir(TEST_FILE)
with Yuna(TEST_FILE, "test", 1, create=True) as db:
    db.new_table("a26", serialize=yuna.SERIALIZE_STR, compress=yuna.COMPRESS_LZ4)
    tbl_a26 = db.tables.a26
    tbl_a26.put("a", "1")
    tbl_a26.put("b", "2")
    #tbl_a26.put("c", "3")
    tbl_a26.put("d", "4")
    tbl_a26.put("e", "5")

    lst = list(tbl_a26.keys())
    print(f"a26 keys: {lst}")
    lst = list(tbl_a26.keys(start='c'))
    print(f"a26 keys from 'c': {lst}")
    lst = list(tbl_a26.keys(start='c', stop='e'))
    print(f"a26 keys from 'c' to < 'e': {lst}")

    lst = list(tbl_a26.items())
    print(f"a26 items: {lst}")
    lst = list(tbl_a26.items(start='c'))
    print(f"a26 items from 'c': {lst}")
    lst = list(tbl_a26.items(start='c', stop='e'))
    print(f"a26 items from 'c' to < 'e': {lst}")

    lst = list(tbl_a26.values())
    print(f"a26 values: {lst}")
    lst = list(tbl_a26.values(start='c'))
    print(f"a26 values from 'c': {lst}")
    lst = list(tbl_a26.values(start='c', stop='e'))
    print(f"a26 values from 'c' to < 'e': {lst}")

    assert tbl_a26.get("a", None) == "1"
    assert tbl_a26.get("b", None) == "2"
    assert tbl_a26.get("c", None) is None
    tbl_j = db.new_table("jjj", serialize=yuna.SERIALIZE_JSON)
    D_FOO = {"f": 0, "o": 1}
    D_BAR = {"b": 9, "a": 8, "r": 7}
    tbl_j.put("foo", D_FOO)
    tbl_j.put("bar", D_BAR)
    assert tbl_j.get("foo", None) == D_FOO
    assert tbl_j.get("bar", None) == D_BAR
    assert tbl_j.get("baz", None) is None

    db.reserved.put("foo", D_FOO)
    assert db.reserved.get("foo", None) == D_FOO
    temp = db.reserved.raw_get(b"foo")
    db.reserved.raw_put(b"bar", temp)
    temp = db.reserved.get("bar")
    assert temp == D_FOO
    db.reserved.raw_delete(b"bar")
    assert db.reserved.get("bar", None) is None

    print("reserved raw_keys:")
    for bytes_key in db.reserved.raw_keys():
        print(bytes_key)
    print()
    print("reserved raw_items:")
    for bytes_key, bytes_value in db.reserved.raw_items():
        print(bytes_key, "->", bytes_value)
    print()
    print("reserved raw_values:")
    for bytes_value in db.reserved.raw_values():
        print(bytes_value)
    print()

    db.reserved.delete("foo")
    assert db.reserved.get("foo", None) is None

with Yuna(TEST_FILE, "test", 1, read_only=False) as db:
    assert db.tables.a26.get("a", None) == "1"
    assert db.tables.a26.get("b", None) == "2"
    assert db.tables.a26.get("c", None) is None
    assert db.tables.jjj.get("foo", None) == D_FOO
    assert db.tables.jjj.get("bar", None) == D_BAR
    assert db.tables.jjj.get("baz", None) is None
    db.tables.jjj.truncate()
    assert db.tables.jjj.get("foo", None) is None
    assert db.tables.jjj.get("bar", None) is None
    assert db.tables.jjj.get("baz", None) is None
    db.tables.jjj.drop()
    db.tables.a26.drop()
