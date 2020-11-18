# -- coding: utf-8 --

'''
    将wind的数据导入到本地数据库,并从数据库返回结果
'''

from WindPy import w
import pandas as pd
from GetAndSaveWindData.MysqlCon import MysqlCon
from iFinDPy import *
from GetAndSaveWindData.GetDataToMysql import GetDataToMysql
import mylog as mylog


class GetDataFromWindAndMySql:
    def __init__(self):
        self.wsetData = ["000001.SH", "399300.SZ", "000016.SH", "000905.SH", "000906.SH"]  # 要获取数据的证券代码
        self.indexFieldName = ["open", "high", "low", "close", "volume", "amt", "chg", "pct_chg", "turn"]  # 要获取的数据字段
        self.fundFieldName = ["nav", "NAV_acc", "sec_name"]
        self.monetaryFund = ["mmf_annualizedyield", "mmf_unityield", "sec_name"]
        self.stockFieldName = ["open", "high", "low", "close", "volume", "amt", "turn", "mkt_cap_ard", "pe_ttm",
                               "ps_ttm", "pb_lf"]
        self.engine = MysqlCon().getMysqlCon(flag='engine')
        self.conn = MysqlCon().getMysqlCon(flag='connect')
        self.GetDataToMysqlDemo = GetDataToMysql()
        self.logger = mylog.set_log()
        # w.start()

        log_state = THS_iFinDLogin('zszq5072', '754628')
        if log_state == 0:
            self.logger.info("同花顺账号登录成功！")
        else:
            self.logger.error("同花顺账号登录异常，请检查！")
            return

    def getStockMonthToMySql(self):
        start_date = '2011-11-01'
        end_date = '2019-12-31'
        total_trade_list = self.getTradeDay(startDate=start_date, endDate=end_date, Period='M')
        wsetdata = w.wset("sectorconstituent", "date=%s;sectorid=a001010100000000" % total_trade_list[0])
        if wsetdata.ErrorCode != 0:
            self.logger.debug("获取全A股数据有误，错误代码" + str(wsetdata.ErrorCode))
        index_df = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields, columns=wsetdata.Codes).T
        if index_df.empty:
            return

        for trade_date in total_trade_list:
            optionstr = "tradeDate=%s;cycle=M" % (trade_date[:4] + trade_date[5:7] + trade_date[8:])
            wssdata = w.wss(codes=index_df['wind_code'].tolist(),
                            fields=["mkt_freeshares", "pe_ttm", "ps_ttm", "pct_chg"],
                            options=optionstr)
            if wssdata.ErrorCode != 0:
                self.logger.debug("获取因子数据有误，错误代码" + str(wssdata.ErrorCode))
                return pd.DataFrame()
            resultDf = pd.DataFrame(wssdata.Data, index=wssdata.Fields, columns=wssdata.Codes).T

            df_list = []
            for col in resultDf:
                temp_df = pd.DataFrame(resultDf[col].values, index=resultDf.index, columns=['factor_value'])
                temp_df['update_time'] = trade_date
                temp_df['stock_code'] = resultDf.index.tolist()
                temp_df['factor_name'] = col
                df_list.append(temp_df)
            total_fa_df = pd.concat(df_list, axis=0, sort=True)
            self.GetDataToMysqlDemo.GetMain(total_fa_df, 'stock_factor_month_value')
            self.logger.info("存储日期%s因子数据成功！" % trade_date)

    def getFactorValue(self, code_list=[], factor_list=[], start_date='2019-04-01', end_date='2019-05-01'):
        # 获取截面因子数据
        sqlStr = "select * from stock_factor_month_value where stock_code in %s and factor_name in %s and update_time='%s'" % (
            str(tuple(code_list)), str(tuple(factor_list)), start_date)
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)

        if resultDf.empty:
            dateFormat = start_date[:4] + start_date[5:7] + start_date[8:]
            wssdata = w.wss(codes=code_list, fields=factor_list, options="unit=1;tradeDate=%s" % dateFormat)
            if wssdata.ErrorCode != 0:
                self.logger.debug("获取因子数据有误，错误代码" + str(wssdata.ErrorCode))
                return pd.DataFrame()
            resultDf = pd.DataFrame(wssdata.Data, index=wssdata.Fields, columns=wssdata.Codes).T
        else:
            df_list = []
            for factor, temp_df in resultDf.groupby(by='factor_name'):
                temp = pd.DataFrame(temp_df['factor_value'].values, index=temp_df['stock_code'], columns=[factor])
                df_list.append(temp)
            resultDf = pd.concat(df_list, sort=True, axis=1)
            return resultDf
        return resultDf

    def getIndexConstituent(self, indexCode='000300.SH', getDate='2019-06-06'):
        '''
        获取指数成分股
        :param indexCode:
        :param getDate:
        :return:
        '''
        if indexCode != '全A股':
            sqlStr = "select * from index_constituent where index_code='%s' and update_time='%s'" % (indexCode, getDate)
            resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
            if resultDf.empty:
                wsetdata = w.wset("indexconstituent", "date=%s;windcode=%s" % (getDate, indexCode))
                if wsetdata.ErrorCode != 0:
                    self.logger.error("获取指数成分股数据有误，错误代码" + str(wsetdata.ErrorCode))
                    return pd.DataFrame()

                resultDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields).T
                if resultDf.empty:
                    wsetdata = w.wset("sectorconstituent", "date=%s;windcode=%s" % (getDate, indexCode))
                    if wsetdata.ErrorCode != 0:
                        self.logger.error("获取板块指数成分股数据有误，错误代码" + str(wsetdata.ErrorCode))
                        return pd.DataFrame()

                    resultDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields).T
                    if resultDf.empty:
                        self.logger.info("指定日期内，未找到有效成分股数据")
                    return resultDf

                dateList = [datetampStr.strftime('%Y-%m-%d') for datetampStr in resultDf['date'].tolist()]
                resultDf['date'] = dateList
                nameDic = {'date': 'adjust_time', 'wind_code': 'stock_code', "sec_name": 'stock_name',
                           'i_weight': 'stock_weight'}
                resultDf.rename(columns=nameDic, inplace=True)
                resultDf['update_time'] = getDate
                resultDf['index_code'] = indexCode
                if 'industry' in resultDf:
                    resultDf.drop(labels='industry', inplace=True, axis=1)
                self.GetDataToMysqlDemo.GetMain(resultDf, 'index_constituent')
        else:
            sqlStr = "select distinct stock_code from stock_factor_month_value where update_time='%s'" % getDate
            resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        return resultDf

    def getLackDataToMySql(self, tempCode, startDate, endDate):
        tableStr = 'index_value'
        codeName = 'index_code'

        sqlStr = "select max(update_time),min(update_time) from %s where %s='%s'" % (tableStr, codeName, tempCode)
        cursor = self.conn.cursor()
        cursor.execute(sqlStr)
        dateStrTuple = cursor.fetchall()[0]
        maxDate = dateStrTuple[0]
        minDate = dateStrTuple[1]

        if not maxDate:
            self.get_hq_data_from_ths(tempCode, startDate=startDate, endDate=endDate)
            return

        if endDate < minDate or startDate > minDate:
            self.get_hq_data_from_ths(tempCode, startDate=startDate, endDate=endDate)
        elif startDate <= minDate:
            if minDate <= endDate < maxDate:
                if startDate != minDate:
                    self.get_hq_data_from_ths(tempCode, startDate=startDate, endDate=minDate)
            elif endDate >= maxDate:
                self.get_hq_data_from_ths(tempCode, startDate=startDate, endDate=minDate)
                if endDate != maxDate:
                    self.get_hq_data_from_ths(tempCode, startDate=maxDate, endDate=endDate)
        elif endDate > maxDate:
            self.get_hq_data_from_ths(tempCode, startDate=maxDate, endDate=endDate)

    def getDataFromMySql(self, tempCode, startDate, endDate, tableFlag='index', nameList=['close_price']):
        if not nameList:
            self.logger.error('传入获取指数的字段不合法，请检查！')
        if tableFlag == 'index':
            tableStr = 'index_value'
            codeName = 'index_code'
        elif tableFlag == 'fund':
            codeName = 'fund_code'
            tableStr = 'fund_net_value'
        elif tableFlag == 'stock':
            codeName = 'stock_code'
            tableStr = 'stock_hq_value'
        elif tableFlag == 'private':
            codeName = 'fund_code'
            tableStr = 'private_net_value'
        elif tableFlag == 'monetary_fund':
            codeName = 'fund_code'
            tableStr = 'monetary_fund'

        sqlStr = "select %s,update_time from %s where %s='%s' and  update_time>='%s'" \
                 " and update_time<='%s'" % (','.join(nameList), tableStr, codeName, tempCode, startDate, endDate)
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        resultDf = resultDf.drop_duplicates('update_time').sort_index()
        resultDf.set_index(keys='update_time', inplace=True, drop=True)
        return resultDf

    def getCurrentNameData(self, tempCodeList, startDate, endDate, tableFlag='stock', nameStr='close_price'):
        '''
        获取指定字段的数据
        '''
        if tableFlag == 'stock':
            totalCodeStr = ''
            for stockCode in tempCodeList:
                totalCodeStr = totalCodeStr + stockCode + "','"

            sqlStr1 = "select max(update_time),min(update_time) from stock_hq_value where stock_code in ('%s')" % totalCodeStr[
                                                                                                                  :-3]
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
                     "and update_time>='%s'" % (nameStr, totalCodeStr, endDate, startDate)
            resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
            dfList = []
            for code, tempDf in resultDf.groupby('stock_code'):
                df = pd.DataFrame(tempDf[nameStr].values, index=tempDf['update_time'], columns=[code])
                dfList.append(df)
            resultDf = pd.concat(dfList, axis=1)
            return resultDf

    def getCurrentDateData(self, tempCodeList, getDate, tableFlag='stock', nameList=['close_price']):
        '''
        获取指定日期的截面数据
        :return:
        '''
        if tableFlag == 'stock':
            totalCodeStr = ""
            for stockCode in tempCodeList:
                totalCodeStr = totalCodeStr + stockCode + "','"

            sqlStr = "select * from stock_hq_value where stock_code in ('%s') and update_time='%s'" % (
                totalCodeStr[:-3], getDate)
            resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
            if resultDf.empty:
                codes = tempCodeList
                fields = self.stockFieldName
                tradeDate = getDate
                wssData = w.wss(codes=codes, fields=fields, options="tradeDate=%s;priceAdj=F;cycle=D" % tradeDate)
                if wssData.ErrorCode != 0:
                    self.logger.error("获取行情数据有误，错误代码" + str(wssData.ErrorCode))
                    return pd.DataFrame()
                tempDf = pd.DataFrame(wssData.Data, index=fields, columns=codes).T
                tempDf.dropna(inplace=True)
                if tempDf.empty:
                    self.logger.error("当前日期%s无行情" % getDate)
                    return pd.DataFrame()

                tempDf['update_time'] = getDate
                nameDic = {"open": "open_price", "high": "high_price", "low": "low_price", "close": "close_price",
                           "mkt_cap_ard": "market_value", }
                tempDf.rename(columns=nameDic, inplace=True)

                tempDf['stock_code'] = tempDf.index.tolist()
                self.GetDataToMysqlDemo.GetMain(tempDf, 'stock_hq_value')
                returnDf = tempDf[nameList]
                return returnDf
            else:
                resultDf.set_index('stock_code', drop=True, inplace=True)
                returnDf = resultDf[nameList]
                return returnDf

    def getFirstDayData(self, codeList, tableFlag='fund'):
        if tableFlag == 'fund':
            wssData = w.wss(codes=codeList, fields=["fund_setupDate"])
            if wssData.ErrorCode != 0:
                self.logger.error("getFirstDayData获取wind数据错误，错误代码%s" % wssData.ErrorCode)
                return pd.DataFrame()
            self.logger.debug("getFirstDayData获取wind数据成功！")
            resultDf = pd.DataFrame(wssData.Data, columns=wssData.Codes, index=wssData.Fields)
            return resultDf

    def get_hq_data_from_ths(self, tempCode, startDate='2019-04-01', endDate='2019-04-30', ):
        ths_data = THS_HistoryQuotes(thscode=tempCode,
                                     jsonIndicator='open,high,low,close,volume,changeRatio,turnoverRatio,change',
                                     jsonparam='Interval:D,CPS:6,baseDate:1900-01-01,Currency:YSHB,fill:Previous',
                                     begintime=startDate,
                                     endtime=endDate, outflag=True)
        if ths_data['errorcode'] != 0:
            self.logger.error("同花顺获取行情数据错误，请检查:%s" % ths_data['errmsg'])
            return

        tempDf = THS_Trans2DataFrame(ths_data)
        tempDf.dropna(how='all', inplace=True)
        tempDf[codeName] = tempCode
        tempDf['update_time'] = tempDf.index.tolist()
        tempDf.rename(columns=nameDic, inplace=True)
        dateList = [dateStr.strftime("%Y-%m-%d") for dateStr in tempDf['update_time'].tolist()]
        tempDf['update_time'] = dateList
        self.GetDataToMysqlDemo.GetMain(tempDf, 'index_value')
        return tempDf

    def getHQData(self, tempCode, startDate='2019-03-01', endDate='2019-05-30', tableFlag='index',
                  nameList=['close_price']):
        '''
        #获取指数行情数据入口
        '''
        self.getLackDataToMySql(tempCode, startDate, endDate, tableFlag)
        resultDf = self.getDataFromMySql(tempCode, startDate, endDate, tableFlag=tableFlag, nameList=nameList)
        return resultDf

    def getTradeDay(self, startDate, endDate, Period='M'):
        '''
        获取指定周期交易日,封装wind接口
        :param Period: ''日，W周，M月，Q季，S半年，Y年
        :return:
        '''
        # w.start()
        data = w.tdays(beginTime=startDate, endTime=endDate, options="Period=%s" % Period)
        if data.ErrorCode != 0:
            self.logger.error('wind获取交易日期错误，请检查！')
            return
        tradeDayList = data.Data[0]
        tradeDayList = [tradeDay.strftime('%Y-%m-%d') for tradeDay in tradeDayList]
        # w.close()
        return tradeDayList


if __name__ == '__main__':
    GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
    # aa = GetDataFromWindAndMySqlDemo.getHQData(tempCode='000300.SH', startDate='2019-02-01', endDate='2019-05-01')
    # aa = GetDataFromWindAndMySqlDemo.getIndexConstituent(indexCode='000905.SH',getDate='2010-02-03')
    # getHQData(self, tempCode, startDate='2019-04-01', endDate='2019-04-30', tableFlag='index',
    #           nameList=['close_price']):
    # aa = GetDataFromWindAndMySqlDemo.getHQData(tempCode='300033.SZ',tableFlag='stock',startDate='2010-01-01',endDate='2010-02-01')
    # aa = GetDataFromWindAndMySqlDemo.getCurrentDateData(tempCodeList=['300033.SZ','600000.SH'],getDate='2012-03-08')
    GetDataFromWindAndMySqlDemo.getStockMonthToMySql()
