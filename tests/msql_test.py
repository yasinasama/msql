# -*- coding: utf-8 -*-

from msql import client

if __name__=='__main__':
    mg = client.Mongo('127.0.0.1',27017,'cj','user')
    for i in mg.execute():
        print(i)
    # print(len(mg.execute()))