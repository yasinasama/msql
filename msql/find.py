import json
from itertools import groupby


class Find:

    c = {
        '>': '$gt',
        '>=': '$gte',
        '<': '$lt',
        '<=': '$lte',
        '!=': '$ne'
    }

    def __init__(self,columns,conditions=None):
        self.columns = columns
        self.conditions = conditions

    def _select(self):
        n = {}

        _id = False
        for column in self.columns:
            name = column.get('name')
            if name == '*':
                return None
            if name.lower() in ['id','_id']:
                _id = True
                continue
            n[name] = 1

        if not _id:
            n['_id'] = 0
        print(n)
        return n

    def _filter(self,conditions):
        m = {}
        # filter
        while isinstance(conditions, list) and len(conditions) == 1:
            conditions = conditions[0]

        if isinstance(conditions, list):
            # `AND` has high priority,so we should split `OR` first
            if 'OR' in conditions:
                k, b = '$or', 'OR'
            else:
                k, b = '$and', 'AND'

            subconditions = self._split_list(conditions, b)
            for sub in subconditions:
                m.setdefault(k, []).append(self._filter(sub))
        else:
            name, value, comp = conditions['name'], conditions['value'], conditions['compare']
            if comp == '=':
                m[name] = value
            elif comp == 'LIKE':
                if not value.startswith('%'):
                    value = '^' + value
                if not value.endswith('%'):
                    value += '$'
                regex = value.strip('%').replace('%', '*')
                m[name] = {'$regex': regex}
            else:
                m[name] = {self.c[comp]: value}
        print(m)
        return m

    def _split_list(self,source,wd):
        return [list(g) for k, g in groupby(source, lambda x: x == wd) if not k]

    def find(self):
        _m = self._filter(self.conditions) if self.conditions else None
        _n = self._select()
        return _m,_n


class Columns:
    def __init__(self,columns):
        self.__columns = []
        self.__functions = []
        self.__id = False
        self.__star = False

        for column in columns:
            name = column.get('name')
            if isinstance(name,dict):
                self.__functions.append(list(name.items()))
            else:
                if name == '*':
                    self.__star = True
                if name.lower() == 'id':
                    self.__id = True
                self.__columns.append(name)

    def has_star(self):
        return self.__star

    def has_id(self):
        return self.__id

    @property
    def columns(self):
        return self.__columns

    @property
    def functions(self):
        return self.__functions




