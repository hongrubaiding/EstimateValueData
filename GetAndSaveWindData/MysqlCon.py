# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

import pymysql
from sqlalchemy import create_engine
import numpy as np

class MysqlCon:
    def __init__(self):
        pass

    def getMysqlCon(self,flag='connect'):
        db_port = 3306
        db_user = 'root'
        db_pass = '123456'
        db_host = 'localhost'
        db_database = 'fund_est'

        pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
        pymysql.converters.conversions = pymysql.converters.encoders.copy()
        pymysql.converters.conversions.update(pymysql.converters.decoders)
        if flag=='connect':
            engine = pymysql.connect(host=db_host, user=db_user, passwd=db_pass, db=db_database, port=db_port)

        elif flag=='engine':
            sqlConStr = "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"%(db_user,db_pass,db_host,db_port,db_database)
            engine = create_engine(sqlConStr)
        return engine

if __name__=='__main__':
    MysqlConDemo = MysqlCon()
    MysqlConDemo.getMysqlCon()