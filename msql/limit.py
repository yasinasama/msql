

class Limit:
    def __init__(self,limit):
        self.skip = limit[0]
        self._limit = limit[1]

    def limit(self):
        return self.skip,self._limit