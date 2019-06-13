# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com
'''
fama french 三因素回归分析
'''

import pandas as pd
import numpy as np
from GetDataFromWindAndMySql import GetDataFromWindAndMySql
from PrintInfo import PrintInfo
import time

class FamaFrenchRegression:
    def __init__(self):
        self.GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
        self.PrintInfoDemo = PrintInfo()

    def getFacrotReturn(self,resultPath,dateList, indexCode):
        totalCodeSet = set({})
        dicTempResult = {}
        self.PrintInfoDemo.PrintLog("获取宽基指数成分股，并计算产品起止日期内规模因子，账面市值因子的收益" )
        self.PrintInfoDemo.PrintLog("为减少接口频繁请求成分股数据导致掉线，这里每次调用后采用睡眠函数，间隔0.2秒..")
        for dateStr in dateList:
            universeDf = self.GetDataFromWindAndMySqlDemo.getIndexConstituent(indexCode=indexCode, getDate=dateStr)
            totalCodeSet = totalCodeSet.union(universeDf['stock_code'].to_dict().values())
            tempStockDf = self.GetDataFromWindAndMySqlDemo.getCurrentDateData(
                tempCodeList=universeDf['stock_code'].tolist(), getDate=dateStr, tableFlag='stock',
                nameList=['close_price', 'market_value', 'pb_lf'])
            if tempStockDf.empty:
                continue

            dicTempResult[dateStr] = {}
            ME30 = np.percentile(tempStockDf['market_value'], 30)
            ME70 = np.percentile(tempStockDf['market_value'], 70)
            SM = tempStockDf[tempStockDf['market_value'] <= ME30].index.tolist()
            BM = tempStockDf[tempStockDf['market_value'] > ME70].index.tolist()

            BP = tempStockDf[tempStockDf > 0].dropna()
            BP[['pb_lf']] = 1 / BP[['pb_lf']]
            BP30 = np.percentile(BP['pb_lf'], 30)
            BP70 = np.percentile(BP['pb_lf'], 70)
            LP = BP[BP['pb_lf'] <= BP30].index.tolist()
            HP = BP[BP['pb_lf'] > BP70].index.tolist()
            dicTempResult[dateStr]['SM'] = SM
            dicTempResult[dateStr]['BM'] = BM
            dicTempResult[dateStr]['LP'] = LP
            dicTempResult[dateStr]['HP'] = HP
            time.sleep(0.2)
        self.PrintInfoDemo.PrintLog("产品起止日期内规模因子，账面市值因子的收益计算完成")
        self.PrintInfoDemo.PrintLog("批量获取产品起止日期内的所有成分股行情数据...")
        totalStockCloseDf = self.GetDataFromWindAndMySqlDemo.getCurrentNameData(tempCodeList=list(totalCodeSet),
                                                                                startDate=dateList[0],
                                                                                endDate=dateList[-1], tableFlag='stock',
                                                                                nameStr='close_price')

        self.PrintInfoDemo.PrintLog("产品起止日期内的所有成分股行情数据获取完成！")
        dateSort = sorted(dicTempResult.items(), key=lambda x: x[0], reverse=False)
        dicResult = {}
        for num in range(1, len(dateSort)):
            dateStr = dateSort[num][0]
            preDateStr = dateSort[num - 1][0]
            dicCodeList = dateSort[num][1]
            dicResult[dateStr] = {}
            SMReturn = (totalStockCloseDf.ix[dateStr, dicCodeList['SM']] - totalStockCloseDf.ix[preDateStr, dicCodeList['SM']]) / \
            totalStockCloseDf.ix[preDateStr, dicCodeList['SM']]
            SMMeanReturn = SMReturn.mean()

            BMReturn = (totalStockCloseDf.ix[dateStr, dicCodeList['BM']] - totalStockCloseDf.ix[
                preDateStr, dicCodeList['BM']]) / \
                       totalStockCloseDf.ix[preDateStr, dicCodeList['BM']]
            BMMeanReturn = BMReturn.mean()

            LPReturn = (totalStockCloseDf.ix[dateStr, dicCodeList['LP']] - totalStockCloseDf.ix[
                preDateStr, dicCodeList['LP']]) / \
                       totalStockCloseDf.ix[preDateStr, dicCodeList['LP']]
            LPMeanReturn = LPReturn.mean()

            HPReturn = (totalStockCloseDf.ix[dateStr, dicCodeList['HP']] - totalStockCloseDf.ix[
                preDateStr, dicCodeList['HP']]) / \
                       totalStockCloseDf.ix[preDateStr, dicCodeList['HP']]
            HPMeanReturn = HPReturn.mean()
            dicResult[dateStr]['SMB'] = SMMeanReturn-BMMeanReturn
            dicResult[dateStr]['HML'] = LPMeanReturn - HPMeanReturn

        resultDf = pd.DataFrame(dicResult).T
        resultDf.to_excel(resultPath+'规模因子账面市值因子（%s成分股）.xlsx'%indexCode)
        self.PrintInfoDemo.PrintLog("产品起止日期内的SMB,HML收益率计算完成，存入本地！")
        return resultDf

    def calcMain(self, closePriceSe, resultPath,indexCode='000016.SH',):
        self.PrintInfoDemo.PrintLog("开始计算fama-french三因子模型,采用的宽基指数为%s"%indexCode)
        tempReturn = (closePriceSe - closePriceSe.shift(1)) / closePriceSe.shift(1)
        tempReturn.name = closePriceSe.name
        dateList = tempReturn.index.tolist()
        factorReturnDf = self.getFacrotReturn(resultPath,dateList=dateList, indexCode=indexCode)
        calcRusultDf = pd.concat([factorReturnDf,tempReturn],axis=1,join='inner')
        calcRusultDf.to_excel(resultPath+'三因子样本数据.xlsx')


if __name__ == '__main__':
    FamaFrenchRegressionDemo = FamaFrenchRegression()
    FamaFrenchRegressionDemo.calcMain()
