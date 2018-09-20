# -- coding: utf-8 --

'''
    将wind的数据导入到本地数据库
'''

from WindPy import w
import pymysql
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine


class GetWindDataToMySql:
    def __init__(self):
        # 数据库配置文件
        self.dataBaseConfig = {}
        self.dataBaseConfig['host'] = 'localhost'
        self.dataBaseConfig['user'] = 'root'
        self.dataBaseConfig['password'] = '123456'
        self.dataBaseConfig['database'] = 'fund_data'

        self.wsetData = ["000001.SH", "399300.SZ", "000016.SH", "000905.SH", "000906.SH"]  # 要获取数据的证券代码
        self.dataName = ["open", "high", "low", "close", "volume", "amt", "chg", "pct_chg", "turn"]  # 要获取的数据字段

    #数据库连接
    def connectMysql(self,dataBase='newOpen'):
        if dataBase == 'newOpen':
            db = pymysql.connect(host=self.dataBaseConfig['host'], user=self.dataBaseConfig['user'],
                                 passwd=self.dataBaseConfig['password'], db=self.dataBaseConfig['database'])
            return db
        else:
            dataBase.close()
            return

    #日志信息打印
    def PrintInfo(self, infostr, otherInfo=''):
        currenttime = datetime.now().strftime('%H:%M:%S')
        if isinstance(otherInfo, str):
            if not otherInfo:
                print(currenttime + '[INFO]:' + infostr)
            else:
                print(currenttime + '[INFO]:' + infostr, otherInfo)
        else:
            print(currenttime + '[INFO]:' + infostr, otherInfo)

    # 获取数据的开始日期
    def getDataStartDate(self):
        db = self.connectMysql()
        cursor = db.cursor()
        sqlStr = "select max(`UPDATE`) from index_data"
        cursor.execute(sqlStr)
        data = cursor.fetchone()[0]
        if not data:
            startDate = '2007-01-01'
        else:
            startDate = data
        self.connectMysql(dataBase=db)
        self.PrintInfo("获取数据的开始日期 : %s" % startDate)
        return startDate

    # 从wind获取数据
    def getDataFromWind(self,startDate):
        totalData = {}
        w.start()
        for code in self.wsetData:
            self.PrintInfo("获取当前指数的历史数据 : %s" % code)
            wsetdata = w.wsd(codes=code, fields=self.dataName, beginTime=startDate)
            if wsetdata.ErrorCode != 0:
                self.PrintInfo("获取当前指数的历史数据异常 : %s" % code)
                continue

            tempDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields, columns=wsetdata.Times).T
            tempDf['CODE'] = code
            tempDf['UPDATE'] = wsetdata.Times
            totalData[code] = tempDf
        w.close()
        return totalData

    #将数据导入到mysql
    def dataToMysql(self,totalData):
        if not totalData:
            self.PrintInfo("未获取到任何有效数据，请检查！" )
            return

        mysqlConfig = ['root', '123456', 'localhost', '3306', 'fund_data', 'utf8']
        mysqlcon = "mysql+pymysql://%s:%s@%s:%s/%s?charset=%s" % (
            mysqlConfig[0], mysqlConfig[1], mysqlConfig[2], mysqlConfig[3], mysqlConfig[4], mysqlConfig[5])
        conn = create_engine(mysqlcon)

        for code,datadf in totalData.items():
            self.PrintInfo('%s历史数据写入数据库。。' % code)
            datadf.to_sql(name='index_data', con=conn, if_exists='append', index=False)

    #运行入口
    def startMain(self):
        startDate = self.getDataStartDate()
        totalData = self.getDataFromWind(startDate)
        self.dataToMysql(totalData)

if __name__=='__main__':
    GetWindDataToMySqlDemo = GetWindDataToMySql()
    GetWindDataToMySqlDemo.startMain()