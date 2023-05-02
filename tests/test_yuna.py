#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import tempfile
import unittest


# Hack sys.path so that tests will run against Yuna from this directory tree,
# even if someone ran "pip install yuna-db" before running these unit tests.
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(THIS_DIR, "../src")))
import yuna


# Data that can be reused in multiple tests
_TEST_DATA = {
    "foo": {'f': 0, 'o': 1},
    "bar": {'b': 9, 'a': 8, 'r': 7},
}


class YunaDBTestCase(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.fname = os.path.join(self.tmpdir.name, "yuna_test.ydb")
        self.db = yuna.Yuna(self.fname, "test", 1, create=True)

    def tearDown(self):
        self.db.close()
        self.tmpdir.cleanup()

    def test_reserved_get_put(self):
        self.db.reserved.put("foo", _TEST_DATA["foo"])
        self.db.reserved.put("bar", _TEST_DATA["bar"])
        tbl_reserved = self.db.reserved

        x = tbl_reserved.get("foo", None)
        self.assertEqual(x, _TEST_DATA["foo"])
        x = tbl_reserved.get("bar", None)
        self.assertEqual(x, _TEST_DATA["bar"])
        x = tbl_reserved.get("baz", None)
        self.assertIsNone(x)

    def test_reserved_delete(self):
        self.db.reserved.put("foo", _TEST_DATA["foo"])
        self.db.reserved.put("bar", _TEST_DATA["bar"])
        tbl_reserved = self.db.reserved

        x = tbl_reserved.get("foo", None)
        self.assertEqual(x, _TEST_DATA["foo"])
        tbl_reserved.delete("foo")
        x = tbl_reserved.get("foo", None)
        self.assertIsNone(x)

        x = tbl_reserved.get("bar", None)
        self.assertEqual(x, _TEST_DATA["bar"])
        tbl_reserved.raw_delete(b"bar")
        x = tbl_reserved.get("bar", None)
        self.assertIsNone(x)

    def test_reserved_keys(self):
        # NOTE: if we don't have any other tables open, reserved will start out empty
        self.db.reserved.put("foo", _TEST_DATA["foo"])
        self.db.reserved.put("bar", _TEST_DATA["bar"])
        tbl_reserved = self.db.reserved

        # keys are listed in sorted order
        x = list(tbl_reserved.keys())
        expected = ["bar", "foo"]
        self.assertEqual(x, expected)

        x = list(tbl_reserved.raw_keys())
        expected = [b"bar", b"foo"]
        self.assertEqual(x, expected)

    def test_reserved_raw_items(self):
        # NOTE: if we don't have any other tables open, reserved will start out empty
        self.db.reserved.put("foo", _TEST_DATA["foo"])
        self.db.reserved.put("bar", _TEST_DATA["bar"])
        tbl_reserved = self.db.reserved

        x = list(tbl_reserved.raw_items())
        expected = [
            (b"bar", yuna.serialize_json(_TEST_DATA["bar"])),
            (b"foo", yuna.serialize_json(_TEST_DATA["foo"]))
        ]
        self.assertEqual(x, expected)

    def test_reserved_raw_values(self):
        # NOTE: if we don't have any other tables open, reserved will start out empty
        self.db.reserved.put("foo", _TEST_DATA["foo"])
        self.db.reserved.put("bar", _TEST_DATA["bar"])
        tbl_reserved = self.db.reserved

        x = list(tbl_reserved.raw_values())
        expected = [
            yuna.serialize_json(_TEST_DATA["bar"]),
            yuna.serialize_json(_TEST_DATA["foo"])
        ]
        self.assertEqual(x, expected)


    def setup_a26(self):
        """
        Create the "a26" table and put data in it that the other tests
        will expect.
        """
        tbl_a26 = self.db.new_table("a26", serialize=yuna.SERIALIZE_STR, compress=yuna.COMPRESS_LZ4)

        tbl_a26.put('a', '1')
        tbl_a26.put('b', '2')
        # skip 'c' on purpose
        tbl_a26.put('d', '4')
        tbl_a26.put('e', '5')

        return tbl_a26

    def test_a26_drop(self):
        tbl_a26 = self.setup_a26()

        self.assertIn("a26", self.db.table_names)
        tbl_a26.drop()
        self.assertNotIn("a26", self.db.table_names)

    def test_a26_get_put(self):
        tbl_a26 = self.setup_a26()

        x = tbl_a26.get('a', None)
        self.assertEqual(x, '1')
        x = tbl_a26.get('b', None)
        self.assertEqual(x, '2')
        x = tbl_a26.get('c', None)
        self.assertIsNone(x)

        import lz4
        import lz4.block
        def a26_pack(x):
            return lz4.block.compress(yuna.serialize_str(x))

        tbl_a26.raw_put(b'c', a26_pack('3'))
        x = tbl_a26.get('c', None)
        self.assertEqual(x, '3')

    def test_a26_delete(self):
        tbl_a26 = self.setup_a26()

        x = tbl_a26.get('a', None)
        self.assertEqual(x, '1')

        tbl_a26.delete('a')
        x = tbl_a26.get('a', None)
        self.assertIsNone(x)

        x = tbl_a26.get('b', None)
        self.assertEqual(x, '2')

        tbl_a26.raw_delete(b'b')
        x = tbl_a26.get('b', None)
        self.assertIsNone(x)

    def test_a26_keys(self):
        tbl_a26 = self.setup_a26()

        x = list(tbl_a26.keys())
        expected = ['a', 'b', 'd', 'e']
        self.assertEqual(x, expected)

        x = list(tbl_a26.raw_keys())
        expected = [b'a', b'b', b'd', b'e']
        self.assertEqual(x, expected)

    def test_a26_items(self):
        import lz4
        import lz4.block
        def a26_unpack(x):
            return yuna.deserialize_str(lz4.block.decompress(x))

        tbl_a26 = self.setup_a26()

        x = list(tbl_a26.items())
        expected = [('a', '1'), ('b', '2'), ('d', '4'), ('e', '5')]
        self.assertEqual(x, expected)

        lst_raw = list(tbl_a26.raw_items())
        lst = [(yuna.deserialize_str(key), a26_unpack(value)) for key, value in lst_raw]
        self.assertEqual(lst, expected)

    def test_a26_values(self):
        import lz4
        import lz4.block
        def a26_unpack(x):
            return yuna.deserialize_str(lz4.block.decompress(x))

        tbl_a26 = self.setup_a26()

        x = list(tbl_a26.values())
        expected = ['1', '2', '4', '5']
        self.assertEqual(x, expected)

        lst_raw = list(tbl_a26.raw_values())
        lst = [a26_unpack(item) for item in lst_raw]
        self.assertEqual(lst, expected)


    def setup_j(self):
        """
        Create the "j" table and put data in it that the other tests
        will expect.
        """
        tbl_j = self.db.new_table("j", serialize=yuna.SERIALIZE_JSON)

        tbl_j.put("foo", _TEST_DATA["foo"])
        tbl_j.put("bar", _TEST_DATA["bar"])

        return tbl_j

    def test_j_drop(self):
        tbl_j = self.setup_j()

        self.assertIn("j", self.db.table_names)
        tbl_j.drop()
        self.assertNotIn("j", self.db.table_names)

    def test_j_get_put(self):
        tbl_j = self.setup_j()

        x = tbl_j.get("foo", None)
        self.assertEqual(x, _TEST_DATA["foo"])
        x = tbl_j.get("bar", None)
        self.assertEqual(x, _TEST_DATA["bar"])
        x = tbl_j.get("baz", None)
        self.assertIsNone(x)

        x = tbl_j.raw_get(b"foo", None)
        self.assertEqual(x, yuna.serialize_json(_TEST_DATA["foo"]))
        x = tbl_j.raw_get(b"bar", None)
        self.assertEqual(x, yuna.serialize_json(_TEST_DATA["bar"]))
        x = tbl_j.raw_get(b"baz", None)
        self.assertIsNone(x)

        BAZ_DATA = {'x': -1, 'y': -2}
        tbl_j.raw_put(b"baz", yuna.serialize_json(BAZ_DATA))
        x = tbl_j.get("baz", None)
        self.assertEqual(x, BAZ_DATA)

    def test_j_delete(self):
        tbl_j = self.setup_j()

        x = tbl_j.get("foo", None)
        self.assertEqual(x, _TEST_DATA["foo"])
        x = tbl_j.delete("foo")
        x = tbl_j.get("foo", None)
        self.assertIsNone(x)

        x = tbl_j.get("bar", None)
        self.assertEqual(x, _TEST_DATA["bar"])
        x = tbl_j.raw_delete(b"bar")
        x = tbl_j.get("bar", None)
        self.assertIsNone(x)

    def test_j_keys(self):
        tbl_j = self.setup_j()

        # keys are listed in sorted order
        x = list(tbl_j.keys())
        expected = ["bar", "foo"]
        self.assertEqual(x, expected)

        x = list(tbl_j.raw_keys())
        expected = [b"bar", b"foo"]
        self.assertEqual(x, expected)

    def test_j_items(self):
        tbl_j = self.setup_j()

        x = list(tbl_j.items())
        expected = [("bar", _TEST_DATA["bar"]), ("foo", _TEST_DATA["foo"])]
        self.assertEqual(x, expected)

        x = list(tbl_j.raw_items())
        expected = [
            (b"bar", yuna.serialize_json(_TEST_DATA["bar"])),
            (b"foo", yuna.serialize_json(_TEST_DATA["foo"]))
        ]
        self.assertEqual(x, expected)

    def test_j_values(self):
        tbl_j = self.setup_j()

        x = list(tbl_j.values())
        expected = [_TEST_DATA["bar"], _TEST_DATA["foo"]]
        self.assertEqual(x, expected)

        x = list(tbl_j.raw_values())
        expected = [
            (yuna.serialize_json(_TEST_DATA["bar"])),
            (yuna.serialize_json(_TEST_DATA["foo"]))
        ]
        self.assertEqual(x, expected)


    def test_yuna_read_only(self):
        pathname = self.db.pathname
        self.setup_a26()
        self.setup_j()
        self.db.close()

        self.db = yuna.YunaReadOnly(pathname, "test", 1)

        x = self.db.tables.a26.get('a', None)
        self.assertEqual(x, '1')

        x = self.db.tables.j.get("foo", None)
        self.assertEqual(x, _TEST_DATA["foo"])


if __name__ == '__main__':
    unittest.main()
