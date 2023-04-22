#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
test_yuna.py -- Test Yuna DB

We need a set of unit tests, but for now we have this.

Exercise several Yuna features to make sure that at least
the basics are working.

"""

import json

from src import yuna
from src.yuna import Yuna
from src.yuna.lmdb_util import _delete_file_or_dir

TEST_FILE = "/tmp/junk.ydb"

#import pdb; pdb.set_trace()
#db = Yuna(TEST_FILE, "test", 1, create=False)

import pdb; pdb.set_trace()
_delete_file_or_dir(TEST_FILE)
db = Yuna(TEST_FILE, "test", 1, create=True)
db.new_table("a26", value_serialize=yuna.SERIALIZE_STR)
tbl_names = db.tables.a26
tbl_names.put("a", "1")
tbl_names.put("b", "2")
assert tbl_names.get("a", None) == "1"
assert tbl_names.get("b", None) == "2"
assert tbl_names.get("c", None) is None
tbl_j = db.new_table("jjj", value_serialize=yuna.SERIALIZE_JSON)
D_FOO = {"f": 0, "o": 1}
D_BAR = {"b": 9, "a": 8, "r": 7}
tbl_j.put("foo", D_FOO)
tbl_j.put("bar", D_BAR)
assert tbl_j.get("foo", None) == D_FOO
assert tbl_j.get("bar", None) == D_BAR
assert tbl_j.get("baz", None) is None

db.reserved.put("foo", D_FOO)
assert db.reserved.get("foo", None) == D_FOO
db.reserved.delete("foo")
assert db.reserved.get("foo", None) is None

db.close()
db = Yuna(TEST_FILE, "test", 1, read_only=False)
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
db.close()
