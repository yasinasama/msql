# -*- coding: utf-8 -*-

from itertools import groupby

import pymongo
from bson.objectid import ObjectId

from .grammar import parse_handle


class Mongo:
    def __init__(self, host, port, db):
        self.host = host
        self.port = port

        self.mongo = pymongo.MongoClient(host, port)
        self.db = self.mongo[db]

        self.asc = pymongo.ASCENDING
        self.desc = pymongo.DESCENDING
         
        self.result = None

    # def objectid_to_str(self):

    def _compare_mapping(self,comp):
        if comp == '>':
            return '$gt'
        if comp == '>=':
            return '$gte'
        if comp == '<':
            return '$lt'
        if comp == '<=':
            return '$lte'
        if comp == '!=':
            return '$ne'

    def _sort_mapping(self,sort):
        return self.asc if sort=='ASC' else self.desc

    def _split_list(self,source,wd):
        return [list(g) for k, g in groupby(source, lambda x: x == wd) if not k]


    def _columns(self,columns):
        c = {}
        need_id = False
        for column in columns:
            n = column['name']
            if n == '*':
                c = {}
                break
            if n == 'id':
                need_id = True
            else:
                c[n] = 1
        if not need_id:
            c['_id'] = 0
        return c

    def _table(self,table):
        self.table = self.db[table]

    def _where(self,where,column):
        d = {}

        if where:
            d = self._make_condsl(where)
        c = self._columns(column)
        self.result = self.table.find(d,c)
            
    def _make_condsl(self,conditions):
        condsl = {}
        # filter 
        while isinstance(conditions, list) and len(conditions) == 1:
            conditions = conditions[0]

        if isinstance(conditions, list):
            # `AND` has high priority,so we should split `OR` first
            if 'OR' in conditions:
                k,b = '$or','OR'
            else:
                k,b = '$and','AND'

            subconditions = self._split_list(conditions,b)
            for sub in subconditions:
                condsl.setdefault(k,[]).append(self._make_condsl(sub))
        else:
            name, value, comp = conditions['name'], conditions['value'], conditions['compare']
            if comp in ('>', '>=', '<', '<=','!='):
                condsl[name] = {self._compare_mapping(comp): value}
            elif comp == '=':
                condsl[name] = value
            elif comp == 'LIKE':
                if not value.startswith('%'):
                    value = '^' + value
                if not value.endswith('%'):
                    value += '$'
                regex = value.strip('%').replace('%','*')
                condsl[name] = {'$regex':regex}
        print(condsl)
        return condsl

    def _order(self,order):
        d = []
        for o in order:
            name,value = o['name'],o['type']
            d.append((name,self._sort_mapping(value)))
        self.reslut = self.result.sort(d)

    def _limit(self,limit):
        self.reslut = self.result.limit(limit)

    def _skip(self,skip):
        self.reslut = self.result.skip(skip)

    def _map(self):
        pass

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

        self._table(table)

        self._where(where,column)

        if order:
            self._order(order)

        if limit:
            self._limit(limit[1])
            self._skip(limit[0])

        return self.result




