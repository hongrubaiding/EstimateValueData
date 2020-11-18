# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    获取数据（excel,wind)存入本地相应数据库表中，
    每张表构建索性，数据存在时，更新，不存在时，插入
'''
import pandas as pd
import numpy as np
from GetAndSaveWindData.MysqlCon import MysqlCon
import mylog as mylog

# self.logger.basicConfig(format="%(asctime)s %(filename)s:%(levelname)s:%(message)s", datefmt="%d-%M-%Y %H:%M:%S",
#                     level=self.logger.DEBUG)

class GetDataToMysql:
    def __init__(self):
        self.conn = MysqlCon().getMysqlCon(flag='connect')
        self.logger = mylog.set_log()
        
    def GetMain(self,dataDf,tableName,):
        # 插入数据语句
        tableList = dataDf.columns.tolist()
        strFormat='%s,'*len(tableList)
        sqlStr = "replace into %s(%s)"%(tableName,','.join(tableList))+"VALUES(%s)"%strFormat[:-1]

        dataDf = dataDf.astype(object).where(pd.notnull(dataDf), None)
        cursor = self.conn.cursor()

        for r in range(0, len(dataDf)):
            values = tuple(dataDf.iloc[r][tableList].tolist())
            cursor.execute(sqlStr, values)

        cursor.close()
        self.conn.commit()
        self.logger.info("数据存入mysql成功！")


if __name__=="__main__":
    GetDataToMysqlDemo = GetDataToMysql()
    GetDataToMysqlDemo.GetMain()