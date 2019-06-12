# -- coding: utf-8 --

'''
    将wind的数据导入到本地数据库,并从数据库返回结果
'''

from WindPy import w
import pandas as pd
from MysqlCon import MysqlCon
from PrintInfo import PrintInfo
from datetime import datetime
w.start()


class GetDataFromWindAndMySql:
    def __init__(self):
        self.wsetData = ["000001.SH", "399300.SZ", "000016.SH", "000905.SH", "000906.SH"]  # 要获取数据的证券代码
        self.indexFieldName = ["open", "high", "low", "close", "volume", "amt", "chg", "pct_chg", "turn"]  # 要获取的数据字段
        self.fundFieldName = ["nav", "NAV_acc", "sec_name"]
        self.stockFieldName = ["open","high","low","close","volume","amt","turn","mkt_cap_ard","pe_ttm","ps_ttm","pb_lf"]
        self.engine = MysqlCon().getMysqlCon(flag='engine')
        self.conn = MysqlCon().getMysqlCon(flag='connect')
        self.PrintInfoDemo = PrintInfo()

    def getIndexConstituent(self,indexCode='000300.SH',getDate='2019-06-06'):
        '''
        获取指数成分股
        :param indexCode:
        :param getDate:
        :return:
        '''
        sqlStr = "select * from index_constituent where index_code='%s' and update_time='%s'"%(indexCode,getDate)
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        if resultDf.empty:
            wsetdata = w.wset("indexconstituent", "date=%s;windcode=%s"%(getDate,indexCode))
            if wsetdata.ErrorCode != 0:
                self.PrintInfoDemo.PrintLog("获取指数成分股数据有误，错误代码" + str(wsetdata.ErrorCode))
                return pd.DataFrame()

            resultDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields).T
            dateList = [datetampStr.strftime('%Y-%m-%d') for datetampStr in resultDf['date'].tolist()]
            resultDf['date'] = dateList
            nameDic = {'date':'adjust_time','wind_code':'stock_code',"sec_name":'stock_name','i_weight':'stock_weight'}
            resultDf.rename(columns=nameDic,inplace=True)
            resultDf['update_time'] = getDate
            resultDf['index_code'] = indexCode

            # 插入数据语句
            tableList = ['update_time','index_code','stock_code','stock_name','stock_weight','adjust_time']
            sqlStr = "replace into index_constituent(update_time,index_code,stock_code,stock_name,stock_weight,adjust_time) " \
                     "VALUES(%s,%s,%s,%s,%s,%s)"
            cursor = self.conn.cursor()
            for r in range(0, len(resultDf)):
                values = tuple(resultDf.ix[r,tableList].tolist())
                cursor.execute(sqlStr, values)
            cursor.close()
            self.conn.commit()
        return resultDf



    def getLackDataToMySql(self, tempCode, startDate, endDate, tableFlag='index'):
        if tableFlag == 'index':
            tableStr = 'index_value'
            codeName = 'index_code'
        elif tableFlag == 'fund':
            tableStr = 'fund_net_value'
            codeName = 'fund_code'
        elif tableFlag == 'stock':
            tableStr='stock_hq_value'
            codeName = 'stock_code'
        sqlStr = "select max(update_time),min(update_time) from %s where %s='%s'" % (tableStr, codeName, tempCode)
        cursor = self.conn.cursor()
        cursor.execute(sqlStr)
        dateStrTuple = cursor.fetchall()[0]
        maxDate = dateStrTuple[0]
        minDate = dateStrTuple[1]

        if not maxDate:
            self.getDataFromWind(tempCode, startDate=startDate, endDate=endDate, tableFlag=tableFlag)
            return

        if endDate < minDate or startDate > minDate:
            self.getDataFromWind(tempCode, startDate=startDate, endDate=endDate, tableFlag=tableFlag)
        elif startDate <= minDate:
            if minDate <= endDate < maxDate:
                self.getDataFromWind(tempCode, startDate=startDate, endDate=minDate, tableFlag=tableFlag)
            elif endDate >= maxDate:
                self.getDataFromWind(tempCode, startDate=startDate, endDate=minDate, tableFlag=tableFlag)
                self.getDataFromWind(tempCode, startDate=maxDate, endDate=endDate, tableFlag=tableFlag)
        elif endDate >= maxDate:
            self.getDataFromWind(tempCode, startDate=maxDate, endDate=endDate, tableFlag=tableFlag)

    def getDataFromWind(self, tempCode, startDate='2019-04-01', endDate='2019-04-30', tableFlag='index'):
        if tableFlag == 'index':
            tableStr = 'index_value'
            nameDic = {"OPEN": "open_price", "HIGH": "high_price", "LOW": "low_price", "CLOSE": "close_price",
                       "VOLUME": "volume", "AMT": "amt", "CHG": "chg", "PCT_CHG": "pct_chg", "TURN": "turn"}
            fields = self.indexFieldName
            codeName = 'index_code'
        elif tableFlag=='fund':
            tableStr = 'fund_net_value'
            nameDic = {"NAV": "net_value", "NAV_ACC": "acc_net_value", "SEC_NAME": "fund_name"}
            fields = self.fundFieldName
            codeName = 'fund_code'
        elif tableFlag=='stock':
            tableStr = 'stock_hq_value'
            nameDic = {"OPEN": "open_price", "HIGH": "high_price", "LOW": "low_price", "CLOSE": "close_price",
                       "VOLUME": "volume", "AMT": "amt", "TURN": "turn", "MKT_CAP_ARD": "market_value", "PE_TTM": "pe_ttm","PS_TTM": "ps_ttm","PB_LF":"pb_lf"}
            fields = self.stockFieldName
            codeName = 'stock_code'

        wsetdata = w.wsd(codes=tempCode, fields=fields, beginTime=startDate, endTime=endDate)
        if wsetdata.ErrorCode != 0:
            self.PrintInfoDemo.PrintLog("获取行情数据有误，错误代码" + str(wsetdata.ErrorCode))
            return

        tempDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields, columns=wsetdata.Times).T
        tempDf[codeName] = tempCode
        tempDf['update_time'] = wsetdata.Times
        tempDf.rename(columns=nameDic, inplace=True)

        # 插入数据语句
        if tableFlag!='stock':
            tempDf.to_sql(tableStr, con=self.engine, index=False, if_exists='append')
        else:
            dateList = [dateStr.strftime("%Y-%m-%d") for dateStr in tempDf['update_time'].tolist()]
            tempDf['update_time'] = dateList
            tableList = ['open_price','high_price','low_price','close_price',
                         'volume','amt','turn','market_value','pe_ttm','ps_ttm','pb_lf','stock_code','update_time']
            sqlStr = "replace into stock_hq_value(open_price,high_price,low_price,close_price,volume,amt,turn," \
                     "market_value,pe_ttm,ps_ttm,pb_lf,stock_code,update_time) " \
                     "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor = self.conn.cursor()
            for r in range(0, len(tempDf)):
                values = tuple(tempDf.ix[r, tableList].tolist())
                cursor.execute(sqlStr, values)
            cursor.close()
            self.conn.commit()
        # w.close()
        return tempDf

    def getDataFromMySql(self, tempCode, startDate, endDate, tableFlag='index', nameList=['close_price']):
        if not nameList:
            self.PrintInfoDemo.PrintLog('传入获取指数的字段不合法，请检查！')

        if tableFlag == 'index':
            tableStr = 'index_value'
            codeName = 'index_code'
        elif tableFlag=='fund':
            codeName = 'fund_code'
            tableStr = 'fund_net_value'
        elif tableFlag=='stock':
            codeName = 'stock_code'
            tableStr = 'stock_hq_value'

        sqlStr = "select %s,update_time from %s where %s='%s' and  update_time>='%s'" \
                 " and update_time<='%s'" % (','.join(nameList), tableStr, codeName, tempCode, startDate, endDate)
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        resultDf.set_index(keys='update_time', inplace=True, drop=True)
        resultDf = resultDf.drop_duplicates().sort_index()
        return resultDf

    def getCurrentNameData(self,tempCodeList,startDate,endDate,tableFlag='stock',nameStr='close_price'):
        '''
        获取指定字段的数据
        '''
        if tableFlag=='stock':
            totalCodeStr=''
            for stockCode in tempCodeList:
                totalCodeStr = totalCodeStr+stockCode+"','"

            sqlStr1= "select max(update_time),min(update_time) from stock_hq_value where stock_code in ('%s')"%totalCodeStr[:-3]
            cursor = self.conn.cursor()
            cursor.execute(sqlStr1)
            dateStrTuple = cursor.fetchall()[0]
            maxDate = dateStrTuple[0]
            minDate = dateStrTuple[1]

            if not maxDate:
                for tempCode in tempCodeList:
                    self.getDataFromWind(tempCode, startDate=startDate, endDate=endDate, tableFlag=tableFlag)
                    return
            else:
                if endDate < minDate or startDate > minDate:
                    for tempCode in tempCodeList:
                        self.getDataFromWind(tempCode, startDate=startDate, endDate=endDate, tableFlag=tableFlag)
                elif startDate <= minDate:
                    if minDate <= endDate < maxDate:
                        for tempCode in tempCodeList:
                            self.getDataFromWind(tempCode, startDate=startDate, endDate=minDate, tableFlag=tableFlag)
                    elif endDate >= maxDate:
                        for tempCode in tempCodeList:
                            self.getDataFromWind(tempCode, startDate=startDate, endDate=minDate, tableFlag=tableFlag)
                            self.getDataFromWind(tempCode, startDate=maxDate, endDate=endDate, tableFlag=tableFlag)
                elif endDate >= maxDate:
                    for tempCode in tempCodeList:
                        self.getDataFromWind(tempCode, startDate=maxDate, endDate=endDate, tableFlag=tableFlag)

            sqlStr = "select %s,update_time,stock_code from stock_hq_value where stock_code in ('%s') and update_time<='%s' " \
                     "and update_time>='%s'" % (nameStr,totalCodeStr,endDate,startDate)
            resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
            dfList=[]
            for code,tempDf in resultDf.groupby('stock_code'):
                df = pd.DataFrame(tempDf[nameStr].values,index=tempDf['update_time'],columns=[code])
                dfList.append(df)
            resultDf = pd.concat(dfList,axis=1)
            return resultDf

    def getCurrentDateData(self,tempCodeList,getDate,tableFlag='stock',nameList=['close_price']):
        '''
        获取指定日期的截面数据
        :return:
        '''
        if tableFlag=='stock':
            totalCodeStr = ""
            for stockCode in tempCodeList:
                totalCodeStr = totalCodeStr+stockCode+"','"

            sqlStr = "select * from stock_hq_value where stock_code in ('%s') and update_time='%s'" % (totalCodeStr[:-3], getDate)
            resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
            if resultDf.empty:
                codes = tempCodeList
                fields=self.stockFieldName
                tradeDate = getDate
                wssData = w.wss(codes=codes,fields=fields,options="tradeDate=%s;priceAdj=F;cycle=D"%tradeDate)
                if wssData.ErrorCode!=0:
                    self.PrintInfoDemo.PrintLog("获取行情数据有误，错误代码" + str(wssData.ErrorCode))
                    return pd.DataFrame()
                tempDf =pd.DataFrame(wssData.Data,index=fields,columns=codes).T
                tempDf.dropna(inplace=True)
                if tempDf.empty:
                    self.PrintInfoDemo.PrintLog("当前日期%s无行情"%getDate)
                    return pd.DataFrame()

                tempDf['update_time'] = getDate
                nameDic = {"open": "open_price", "high": "high_price", "low": "low_price", "close": "close_price",
                           "mkt_cap_ard": "market_value",}
                tempDf.rename(columns=nameDic,inplace=True)

                tempDf['stock_code'] = tempDf.index.tolist()
                tableList = ['open_price', 'high_price', 'low_price', 'close_price',
                             'volume', 'amt', 'turn', 'market_value', 'pe_ttm', 'ps_ttm', 'pb_lf', 'stock_code',
                             'update_time']
                sqlStr = "replace into stock_hq_value(open_price,high_price,low_price,close_price,volume,amt,turn," \
                         "market_value,pe_ttm,ps_ttm,pb_lf,stock_code,update_time) " \
                         "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor = self.conn.cursor()
                for r in range(0, len(tempDf)):
                    values = tuple(tempDf.ix[r, tableList].tolist())
                    cursor.execute(sqlStr, values)
                cursor.close()
                self.conn.commit()
                returnDf = tempDf[nameList]
                return returnDf
            else:
                resultDf.set_index('stock_code',drop=True,inplace=True)
                returnDf = resultDf[nameList]
                return returnDf

    def getHQData(self, tempCode, startDate='2019-03-01', endDate='2019-05-30', tableFlag='index',
                  nameList=['close_price']):
        '''
        #获取指数行情数据入口
        '''
        self.getLackDataToMySql(tempCode, startDate, endDate, tableFlag)
        resultDf = self.getDataFromMySql(tempCode, startDate, endDate, tableFlag=tableFlag, nameList=nameList)
        return resultDf

    def getTradeDay(self, startdate, endDate, Period=''):
        '''
        获取指定周期交易日,封装wind接口
        :param Period: ''日，W周，M月，Q季，S半年，Y年
        :return:
        '''
        # w.start()
        data = w.tdays(beginTime=startdate, endTime=endDate, options="Period=%s" % Period)
        if data.ErrorCode != 0:
            self.PrintInfoDemo.PrintLog('wind获取交易日期错误，请检查！')
            return
        tradeDayList = data.Data[0]
        tradeDayList = [tradeDay.strftime('%Y-%m-%d') for tradeDay in tradeDayList]
        # w.close()
        return tradeDayList


if __name__ == '__main__':
    GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
    # GetDataFromWindAndMySqlDemo.getHQData(indexCode='000300.SH', startDate='2019-02-01', endDate='2019-05-01')
    # aa = GetDataFromWindAndMySqlDemo.getIndexConstituent(indexCode='000905.SH')
    # getHQData(self, tempCode, startDate='2019-04-01', endDate='2019-04-30', tableFlag='index',
    #           nameList=['close_price']):
    # aa = GetDataFromWindAndMySqlDemo.getHQData(tempCode='300033.SZ',tableFlag='stock')
    aa = GetDataFromWindAndMySqlDemo.getCurrentDateData(tempCodeList=['300033.SZ','600000.SH'],getDate='2018-03-08')
    print(aa)