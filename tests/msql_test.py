# -*- coding: utf-8 -*-

from msql import client

if __name__=='__main__':
    mg = client.Mongo('127.0.0.1',27017,'cj')
    for i in mg.execute('select id,userid from user where userid like "a%"'):
        print(i)
    # print(len(mg.execute()))