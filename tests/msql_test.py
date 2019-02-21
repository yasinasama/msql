# -*- coding: utf-8 -*-

import msql

from bson.objectid import ObjectId

import unittest


class Test(unittest.TestCase):
    def setUp(self):
        self.m = msql.Mongo('127.0.0.1',27017,'cj')

    def test_select_all(self):
        sql = 'select * from user'
        self.assertEqual(list(self.m.execute(sql)),[
            {"_id": ObjectId("5c166e9ccb9bebec90d525ec"), "userid": "chenjie", "age": 25},
            {"_id": ObjectId("5c1a53182a77892a9796eefa"), "userid": "zhangsan", "age": 20},
            {"_id": ObjectId("5c239a380c32b208b11d1286"), "userid": "%aa", "age": 20},
            {"_id": ObjectId("5c24ede60c32b208b11d1287"), "userid": "a", "age": 20},
            {"_id": ObjectId("5c6e79117fd3c09e3e541b84"), "userid": "lisi", "age": 15}
        ])

    def test_return_columns(self):
        sql1 = 'select * from user where userid = "chenjie" limit 1'
        sql2 = 'select userid,age from user where userid = "chenjie" limit 1'
        self.assertEqual(self.m.execute(sql1)[0],{'_id': ObjectId('5c166e9ccb9bebec90d525ec'), 'userid': 'chenjie', 'age': 25.0})
        self.assertEqual(self.m.execute(sql2)[0], {'userid': 'chenjie', 'age': 25.0})

    def test_compare(self):
        sql1 = 'select userid from user where userid = "chenjie" limit 1'
        sql2 = 'select userid from user where age > 20 limit 1'
        sql3 = 'select userid from user where userid like "%zhang%" limit 1'
        sql4 = 'select userid from user where userid != "chenjie" limit 1'
        self.assertEqual(self.m.execute(sql1)[0],{'userid': 'chenjie'})
        self.assertEqual(self.m.execute(sql2)[0],{'userid': 'chenjie'})
        self.assertEqual(self.m.execute(sql3)[0], {'userid': 'zhangsan'})
        self.assertEqual(self.m.execute(sql4)[0], {'userid': 'zhangsan'})

    def test_conditions(self):
        sql1 = 'select userid,age from user where userid = "chenjie" and age = 25 limit 1'
        sql2 = 'select userid from user where userid = "chenjie" or userid = "zhangsan" limit 2'
        self.assertEqual(self.m.execute(sql1)[0], {'userid': 'chenjie', "age": 25})
        self.assertEqual(list(self.m.execute(sql2)), [{"userid":"chenjie"},{"userid":"zhangsan"}])

    def test_order(self):
        sql1 = 'select age from user order by age limit 1'
        sql2 = 'select age from user order by age desc limit 1'
        self.assertEqual(self.m.execute(sql1)[0], {"age":15})
        self.assertEqual(self.m.execute(sql2)[0], {"age":25})

    def test_limit(self):
        sql = 'select userid from user limit 1,1'
        self.assertEqual(self.m.execute(sql)[0], {"userid":"zhangsan"})




if __name__=='__main__':
    unittest.main()
