from itertools import groupby


class Where:

    c = {
        '>': '$gt',
        '>=': '$gte',
        '<': '$lt',
        '<=': '$lte',
        '!=': '$ne'
    }

    def __init__(self,conditions=None):
        self.conditions = conditions

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
        return _m