# -*- coding: utf-8 -*-

import pymongo
from bson.objectid import ObjectId

from .grammar import parse_handle


class Mongo:
    def __init__(self, host, port, db, table):
        self.host = host
        self.port = port

        self.mongo = pymongo.MongoClient(host, port)
        self.db = self.mongo[db]
        self.table = self.db[table]

        self.call = None
        self.dsl = None


    def _init_parser(self,sql):
        try:
            self.dsl = parse_handle(sql)
        except:
            raise
    # def objectid_to_str(self):


    def execute(self):
        return self.table.find().limit(1)



        

