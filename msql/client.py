# -*- coding: utf-8 -*-

import pymongo

from .grammar import parse_handle
from .find import Find
from .limit import Limit
from .sort import Sort


class Mongo:
    def __init__(self, host, port, db):
        self.host = host
        self.port = port

        self.mongo = pymongo.MongoClient(host, port)
        self.db = self.mongo[db]

    def execute(self,sql):
        sql = sql.strip(';')+';'
        try:
            dsl = parse_handle(sql)
            print(dsl)
        except:
            raise

        table = dsl['table'][0]['name']
        where = dsl['where']
        column = dsl['column']
        order = dsl['order']
        limit = dsl['limit']

        res = self.db[table]
        if where:
            res = res.find(*Find(column,where).find())

        if order:
            res = res.sort(Sort(order).sort())

        if limit:
            s,l = Limit(limit).limit()
            if s:
                res = res.limit(l)
            if l:
                res = res.skip(s)

        return res




