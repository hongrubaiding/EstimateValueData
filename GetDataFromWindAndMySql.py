# -- coding: utf-8 --

'''
    将wind的数据导入到本地数据库,并从数据库返回结果
'''

from WindPy import w
import pandas as pd
from MysqlCon import MysqlCon
from PrintInfo import PrintInfo
w.start()


class GetDataFromWindAndMySql:
    def __init__(self):
        self.wsetData = ["000001.SH", "399300.SZ", "000016.SH", "000905.SH", "000906.SH"]  # 要获取数据的证券代码
        self.indexFieldName = ["open", "high", "low", "close", "volume", "amt", "chg", "pct_chg", "turn"]  # 要获取的数据字段
        self.fundFieldName = ["nav","NAV_acc","sec_name"]
        self.engine = MysqlCon().getMysqlCon(flag='engine')
        self.conn = MysqlCon().getMysqlCon(flag='connect')
        self.PrintInfoDemo = PrintInfo()

    # 获取缺失数据到Mysql
    def getLackDataToMySql(self, tempCode, startDate, endDate,tableFlag='index'):
        if tableFlag=='index':
            tableStr = 'index_value'
            codeName = 'index_code'
        elif tableFlag == 'fund':
            tableStr = 'fund_net_value'
            codeName ='fund_code'
        sqlStr = "select max(update_time),min(update_time) from %s where %s='%s'" % (tableStr,codeName,tempCode)
        cursor = self.conn.cursor()
        cursor.execute(sqlStr)
        dateStrTuple = cursor.fetchall()[0]
        maxDate = dateStrTuple[0]
        minDate = dateStrTuple[1]

        if not maxDate:
            self.getDataFromWind(tempCode, startDate=startDate, endDate=endDate,tableFlag=tableFlag)
            return

        if endDate < minDate or startDate > minDate:
            self.getDataFromWind(tempCode, startDate=startDate, endDate=endDate,tableFlag=tableFlag)
        elif startDate <= minDate:
            if minDate <= endDate < maxDate:
                self.getDataFromWind(tempCode, startDate=startDate, endDate=minDate,tableFlag=tableFlag)
            elif endDate >= maxDate:
                self.getDataFromWind(tempCode, startDate=startDate, endDate=minDate,tableFlag=tableFlag)
                self.getDataFromWind(tempCode, startDate=maxDate, endDate=endDate,tableFlag=tableFlag)
        elif endDate >= maxDate:
            self.getDataFromWind(tempCode, startDate=maxDate, endDate=endDate,tableFlag=tableFlag)

    # 从wind获取数据
    def getDataFromWind(self,tempCode, startDate='2019-04-01', endDate='2019-04-30',tableFlag='index'):
        if tableFlag=='index':
            tableStr = 'index_value'
            nameDic = {"OPEN": "open_price", "HIGH": "high_price", "LOW": "low_price", "CLOSE": "close_price",
                       "VOLUME": "volume", "AMT": "amt", "CHG": "chg", "PCT_CHG": "pct_chg", "TURN": "turn"}
            fields = self.indexFieldName
            codeName = 'index_code'
        else :
            tableStr = 'fund_net_value'
            nameDic = {"NAV":"net_value","NAV_ACC":"acc_net_value","SEC_NAME":"fund_name"}
            fields = self.fundFieldName
            codeName = 'fund_code'
        wsetdata = w.wsd(codes=tempCode, fields=fields, beginTime=startDate, endTime=endDate)
        if wsetdata.ErrorCode!=0:
            self.PrintInfoDemo.PrintLog("获取行情数据有误，错误代码"+str(wsetdata.ErrorCode))
            return

        tempDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields, columns=wsetdata.Times).T
        tempDf[codeName] = tempCode
        tempDf['update_time'] = wsetdata.Times
        tempDf.rename(columns=nameDic, inplace=True)
        tempDf.to_sql(tableStr, con=self.engine, index=False, if_exists='append')
        # w.close()
        return tempDf

    def getDataFromMySql(self, tempCode, startDate, endDate, tableFlag='index',nameList=['close_price']):
        if not nameList:
            self.PrintInfoDemo.PrintLog('传入获取指数的字段不合法，请检查！')

        if tableFlag=='index':
            tableStr = 'index_value'
            codeName = 'index_code'
        else:
            codeName = 'fund_code'
            tableStr = 'fund_net_value'

        sqlStr = "select %s,update_time from %s where %s='%s' and  update_time>='%s'" \
                 " and update_time<='%s'" % (','.join(nameList),tableStr, codeName, tempCode,startDate, endDate)
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        resultDf.set_index(keys='update_time', inplace=True, drop=True)
        resultDf = resultDf.drop_duplicates().sort_index()
        return resultDf

    def getHQData(self, tempCode, startDate='2019-04-01', endDate='2019-04-30', tableFlag='index',nameList=['close_price']):
        '''
        #获取指数行情数据入口
        '''
        self.getLackDataToMySql(tempCode, startDate, endDate,tableFlag)
        resultDf = self.getDataFromMySql(tempCode, startDate, endDate, tableFlag=tableFlag,nameList=nameList)
        return resultDf

    def getTradeDay(self, startdate, endDate, Period=''):
        '''
        获取指定周期交易日,封装wind接口
        :param Period: ''日，W周，M月，Q季，S半年，Y年
        :return:
        '''
        # w.start()
        data = w.tdays(beginTime=startdate, endTime=endDate, options="Period=%s" % Period)
        if data.ErrorCode!=0:
            self.PrintInfoDemo.PrintLog('wind获取交易日期错误，请检查！')
            return
        tradeDayList = data.Data[0]
        tradeDayList = [tradeDay.strftime('%Y-%m-%d') for tradeDay in tradeDayList]
        # w.close()
        return tradeDayList





if __name__ == '__main__':
    GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
    GetDataFromWindAndMySqlDemo.getHQData(indexCode='000300.SH', startDate='2019-02-01', endDate='2019-05-01')
