# -*- coding: utf-8 -*-

from itertools import groupby

import pymongo
from bson.objectid import ObjectId

from msql.grammar import parse_handle


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

    def _sort_mapping(self,sort):
        return self.asc if sort=='ASC' else self.desc

    def split_list(self,source,wd):
        return [list(g) for k, g in groupby(source, lambda x: x == wd) if not k]

    def _table(self,table):
        self.table = self.db[table]

    def _where(self,where):
        _d = {}

        if where:
            while isinstance(where, list) and len(where) == 1:
                where = where[0]
            print(where)
            for w in where:
                _name,_value,_comp = w['name'],w['value'],w['compare']
                if _comp in ('>','>=','<','<='):
                    _d[_name] = {self._compare_mapping(_comp):_value}
                elif _comp == '=':
                    _d[_name] = _value
        self.result = self.table.find(_d)
            
    def _make_conds(self,conditions,conds=None):
        if not conds:
            conds = {}

        # filter 
        while isinstance(conditions, list) and len(conditions) == 1:
            conditions = conditions[0]

        # `AND` has high priority,so we should split `OR` first
        if isinstance(conditions, list):
            while len(conditions) == 1:
                conditions = conditions[0]
            if 'OR' in conditions:
                ors = conditions.split

        l = ['OR' in conditions,'AND' in conditions]
        if any(l):
            if l[0]:
                subconds = self.split_list(conditions,'OR')
                conds['$or'] = subconds
            else:
                subconds = self.split_list(conditions,'AND')
            for subcond in subconds:
                conds[].append(self._make_conds(subcond))
        else:
            _name, _value, _comp = conditions['name'], conditions['value'], conditions['compare']
            if _comp in ('>', '>=', '<', '<='):
                conds[_name] = {self._compare_mapping(_comp): _value}
            elif _comp == '=':
                conds[_name] = _value
        return conds

    def _order(self,order):
        _d = []
        for o in order:
            _name,_type = o['name'],o['type']
            _d.append((_name,self._sort_mapping(_type)))
        self.reslut = self.result.sort(_d)

    def _limit(self,limit):
        self.reslut = self.result.limit(limit)

    def _skip(self,skip):
        self.reslut = self.result.skip(skip)

    def execute(self,sql):
        sql = sql.strip(';')+';'
        try:
            dsl = parse_handle(sql)
            print(dsl)
        except:
            raise

        table = dsl['table'][0]['name']
        where = dsl['where']
        order = dsl['order']
        limit = dsl['limit']

        self._table(table)
        
        self._where(where)

        if order:
            self._order(order)

        if limit:
            self._limit(limit[0])
            self._skip(limit[1])

        return self.result


if __name__=='__main__':
    mg = Mongo('10.68.120.190',27017,'tt')
    for i in mg.execute('select * from people where age2>100;'):
        print(i)



        

