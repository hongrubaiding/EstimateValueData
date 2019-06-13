# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    获取数据（excel,wind)存入本地相应数据库表中，
    每张表构建索性，数据存在时，更新，不存在时，插入
'''
import pandas as pd
from DataToMySql.MysqlCon import MysqlCon
import numpy as np

class GetDataToMysql:
    def __init__(self):
        self.conn = MysqlCon().getMysqlCon(flag='connect')

    def GetMain(self,dataDf,tableName):
        # 插入数据语句
        tableList = dataDf.columns.tolist()
        strFormat='%s,'*len(tableList)
        sqlStr = "replace into %s(%s)"%(tableName,','.join(tableList))+"VALUES(%s)"%strFormat[:-1]

        cursor = self.conn.cursor()
        # dataDf.replace(np.nan,None)
        try:
            dataDf[dataDf.isnull()] = None
        except:
            a=0
        for r in range(0, len(dataDf)):
            values = tuple(dataDf.ix[r, tableList].tolist())
            try:
                cursor.execute(sqlStr, values)
            except:
                a=0
        cursor.close()
        self.conn.commit()


if __name__=="__main__":
    GetDataToMysqlDemo = GetDataToMysql()
    GetDataToMysqlDemo.GetMain()