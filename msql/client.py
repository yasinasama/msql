# -*- coding: utf-8 -*-

import pymongo

from .grammar import parse_handle
from .columns import Columns
from .where import Where
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

        result = self.db[table]

        pcolumn = Columns(column).dsl()
        pwhere = Where(where).find()
        psort = Sort(order).sort()
        pskip,plimit = Limit(limit).limit()


        return result.find(
            filter=pwhere,
            projection=pcolumn,
            sort=psort,
            skip=pskip,
            limit=plimit
        )




