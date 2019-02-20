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

    def __init__(self,columns,conditions):
        self.columns = columns
        self.conditions = conditions

        self.m = dict()
        self.n = dict()

    def _select(self):
        _id = False
        for column in self.columns:
            name = column.get('name')
            if name == '*':
                self.n.clear()
                break
            if name.lower() in ['id','_id']:
                _id = True
                continue
            self.n[name] = 1

        if not _id:
            self.n['_id'] = 0


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
        return self._filter(self.conditions),self._select()


