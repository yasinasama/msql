# -*- coding: utf-8 -*-

from msql import client

if __name__=='__main__':
    mg = client.Mongo('127.0.0.1',27017,'cj')
    for i in mg.execute('select * from user where (age > 20)'):
        print(i)
    # print(len(mg.execute()))