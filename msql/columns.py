

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
                self.__columns.append(name)
                if name == '*':
                    self.__star = True
                    continue
                if name == '_id':
                    self.__id = True

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

    def dsl(self):
        d = {}
        for c in self.__columns:
            if self.has_star():
                return None
            else:
                d[c] = 1

        if not self.has_id():
            d['_id'] = 0

        return d



