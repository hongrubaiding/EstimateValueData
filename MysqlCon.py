# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com
from configparser import ConfigParser
import pymysql
from sqlalchemy import create_engine

class MysqlCon:
    def __init__(self):
        pass

    def getMysqlCon(self,flag='connect'):
        ConfigParserDemo = ConfigParser()
        ConfigParserDemo.read('mysql.conf')
        db_port = ConfigParserDemo.getint('db', 'db_port')
        db_user = ConfigParserDemo.get('db', 'db_user')
        db_pass = ConfigParserDemo.get('db', 'db_pass')
        db_host = ConfigParserDemo.get('db', 'db_host')
        db_database = ConfigParserDemo.get('db', 'db_database')
        if flag=='connect':
            engine = pymysql.connect(host=db_host, user=db_user, passwd=db_pass, db=db_database, port=db_port)
        elif flag=='engine':
            sqlConStr = "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"%(db_user,db_pass,db_host,db_port,db_database)
            engine = create_engine(sqlConStr)
        return engine

if __name__=='__main__':
    MysqlConDemo = MysqlCon()
    MysqlConDemo.getMysqlCon()