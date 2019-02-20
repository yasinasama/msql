# -*- coding: utf-8 -*-

import msql

if __name__=='__main__':
    mg = msql.Mongo('127.0.0.1',27017,'cj')
    for i in mg.execute('select userid from user where userid like "a%" limit 1'):
        print(i)