import pymongo

class Sort:
    def __init__(self,order):
        self.order = order

        self.m = {
            'ASC': pymongo.ASCENDING,
            'DESC': pymongo.DESCENDING
        }

    def sort(self):
        d = []
        for o in self.order:
            name,value = o['name'],o['type']
            d.append((name,self.m[value]))
        return d

