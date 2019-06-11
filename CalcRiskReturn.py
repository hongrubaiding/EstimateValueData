# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
基于净值类数据的分析
'''


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from datetime import datetime,timedelta

matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['font.family'] = 'sans-serif'
matplotlib.rcParams['axes.unicode_minus'] = False
from DateFormatDf import DateFormatDf


class CalcRiskReturn:
    def __init__(self):
        self.DateFormatDfDemo = DateFormatDf()

    def formaData(self, tempValue, flagP=True):
        if flagP:
            result = str(round(round(tempValue, 4) * 100, 2)) + '%'
        else:
            result = round(tempValue, 2)
        return result

    def calcMaxdown(self, return_list):
        '''最大回撤率'''
        return_list = (return_list + 1).cumprod()
        return_list = return_list.values
        i = np.argmax(np.maximum.accumulate(return_list) - return_list)
        if i == 0:
            return 0
        j = np.argmax(return_list[:i])
        result = (return_list[j] - return_list[i]) / return_list[j]
        return result

    def calcDetail(self, tempValueDf):
        dicResult = {}              #格式化数据输出
        dicRightResult = {}         #原始数据输出
        assetAnnualReturn = (tempValueDf.iloc[-1] / tempValueDf.iloc[0]) ** (250 / tempValueDf.shape[0]) - 1
        tempReturn = (tempValueDf - tempValueDf.shift(1)) / tempValueDf.shift(1)
        tempReturn.fillna(0, inplace=True)

        tempReturnValue = tempReturn.copy()
        tempReturnValue[tempReturnValue > 0] = 0
        assetDownRisk = tempReturnValue.std() * np.sqrt(250)
        assetStd = tempReturn.std() * np.sqrt(250)
        assetMaxDown = tempReturn.apply(self.calcMaxdown)
        assetCalmar = assetAnnualReturn / assetMaxDown
        assetSharp = (assetAnnualReturn - 0.02) / assetStd
        dicResult[u'年化收益'] = assetAnnualReturn.apply(self.formaData)
        dicResult[u'年化波动'] = assetStd.apply(self.formaData)
        dicResult[u'最大回撤'] = assetMaxDown.apply(self.formaData)
        dicResult[u'夏普比率'] = assetSharp.apply(self.formaData, args=(False,))
        dicResult[u'卡玛比率'] = assetCalmar.apply(self.formaData, args=(False,))
        dicResult[u'下行风险'] = assetDownRisk.apply(self.formaData)

        dicRightResult[u'年化收益'] = assetAnnualReturn
        dicRightResult[u'年化波动'] = assetStd
        dicRightResult[u'最大回撤'] = assetMaxDown
        dicRightResult[u'夏普比率'] = assetSharp
        dicRightResult[u'卡玛比率'] = assetCalmar
        dicRightResult[u'下行风险'] = assetDownRisk

        # successSe = len(tempReturn[tempReturn>0])/len(tempReturn)
        # dicResult[u'胜率'] = self.formaData(len(tempReturn[tempReturn>0])/len(tempReturn))
        return dicResult,dicRightResult

    def calcRiskReturn(self, fundIndexDf, resultPath):
        timeWindowList = ['近一月', '近三月', '近六月', '近一年', '成立以来']
        timeWindowNum = [21, 21 * 3, 21 * 6, 21 * 12, np.inf]

        fundDfList = []
        fundRightDfList = []
        for timeWindow in timeWindowList:
            timeNum = timeWindowNum[timeWindowList.index(timeWindow)]

            if timeNum != np.inf:
                tempValueDf = fundIndexDf[-timeNum:]
            else:
                tempValueDf = fundIndexDf

            tempResult,tempRightResult = self.calcDetail(tempValueDf)
            tempDf = pd.DataFrame(tempResult).T
            tempDf['统计周期'] = timeWindow
            tempDf['数据截止日期'] = tempValueDf.index.tolist()[-1]

            tempRightDf = pd.DataFrame(tempRightResult).T
            tempRightDf['统计周期'] = timeWindow
            tempRightDf['数据截止日期'] = tempValueDf.index.tolist()[-1]

            fundDfList.append(tempDf)
            fundRightDfList.append(tempRightDf)
        tempToExcelDf = pd.concat(fundDfList, axis=0)
        tempRightToExcelDf =pd.concat(fundRightDfList, axis=0)
        tempToExcelDf['统计指标'] = tempToExcelDf.index.tolist()
        tempRightToExcelDf['统计指标'] = tempRightToExcelDf.index.tolist()
        tempToExcelDf.set_index(keys=['统计周期', '统计指标'], drop=True, inplace=True)
        tempRightToExcelDf.set_index(keys=['统计周期', '统计指标'], drop=True, inplace=True)

        # tempToExcelDf.rename(columns)
        tempToExcelDf.to_excel(resultPath + '风险收益统计指标.xlsx')
        tempRightToExcelDf.to_excel(resultPath + '风险收益统计指标原始数据.xlsx')

    def plotDayNetValueFigure(self, fundPlotDf, resultPath, fundName,netPeriod=''):
        '''
        累计收益走势，连续回撤率走势，滚动年化波动走势
        :param fundPlotDf:
        :param resultPath:
        :return:
        '''
        fundPlotFormatDf = self.DateFormatDfDemo.getStrToDate(fundPlotDf)
        tempReturn = (fundPlotFormatDf - fundPlotFormatDf.shift(1)) / fundPlotFormatDf.shift(1)
        tempReturn.fillna(0, inplace=True)
        accReturn = (1 + tempReturn).cumprod() - 1

        plt.style.use('ggplot')
        fig = plt.figure(figsize=(16, 9))
        ax = fig.add_subplot(111)
        accReturn.plot(ax=ax)
        ax.grid()
        ax.set_xlabel('时间')
        ax.set_ylabel('收益率')
        ax.set_title('累计收益走势图')
        plt.savefig(resultPath + '累计收益走势图.png')

        def historydownrate(tempdata):
            templist = []
            for k in range(len(tempdata)):
                downrate = tempdata[k] / tempdata[:k + 1].max() - 1
                templist.append(downrate)
            tempdf = pd.Series(templist, index=tempdata.index)
            tempdf.name = tempdata.name
            return tempdf

        tempComDf = tempReturn[[fundName,'沪深300']]
        downDf = (1 + tempComDf).cumprod().apply(historydownrate)
        plt.style.use('ggplot')
        fig1 = plt.figure(figsize=(16, 9))
        ax1 = fig1.add_subplot(111)
        downDf.plot(ax=ax1)
        ax1.grid()
        ax1.set_xlabel('时间')
        ax1.set_ylabel('回撤率')
        ax1.set_title('回撤率走势图')
        plt.savefig(resultPath + '回撤率走势图.png')

        if netPeriod=='W':
            window=4
            calcFreq = 52
        else:
            window=21
            calcFreq=250

        annualStdDf = tempComDf.rolling(window=window).std()*np.sqrt(calcFreq)
        plt.style.use('ggplot')
        fig2 = plt.figure(figsize=(16, 9))
        ax2 = fig2.add_subplot(111)
        annualStdDf.plot(ax=ax2)
        ax2.grid()
        ax2.set_xlabel('时间')
        ax2.set_ylabel('年化波动率')
        ax2.set_title('滚动年化波动率走势图')
        plt.savefig(resultPath + '滚动年化波动率走势图.png')


        dicDf = {}
        totalDateList = fundPlotFormatDf.index.tolist()
        for rolLoc in range(window,fundPlotFormatDf.shape[0]):
            if rolLoc+window<=fundPlotFormatDf.shape[0]:
                calcAnnualDf = fundPlotFormatDf.iloc[rolLoc:rolLoc+window]
                dicDf[totalDateList[rolLoc]] = (calcAnnualDf.iloc[-1] / calcAnnualDf.iloc[0]) ** (calcFreq / calcAnnualDf.shape[0]) - 1
                # dicDf[totalDateList[rolLoc]] = annualReturn
        rollAnnualReturnDf = pd.DataFrame(dicDf).T
        plt.style.use('ggplot')
        fig3 = plt.figure(figsize=(16, 9))
        ax3 = fig3.add_subplot(111)
        rollAnnualReturnDf.plot(ax=ax3)
        ax3.grid()
        ax3.set_xlabel('时间')
        ax3.set_ylabel('滚动年化收益率')
        ax3.set_title('滚动年化收益率走势图')
        plt.savefig(resultPath + '滚动年化收益率走势图.png')
        # plt.show()

    def calcWeekNetValueResult(self, weekFundPlotDf, resultPath, fundName):
        '''
        周度频率数据统计
        :return:
        '''

        def upAndDownTrade(tempSe):
            failTrade = len(tempSe[tempSe < 0]) / len(tempSe)
            successTrade = 1 - failTrade

            totalValue = tempSe.tolist()

            tempFailTimes = 0
            failTimes = 0
            for valueLoc in range(len(tempSe)):
                if totalValue[valueLoc] < 0:
                    tempFailTimes = tempFailTimes + 1
                else:
                    if tempFailTimes >= failTimes:
                        failTimes = tempFailTimes
                    tempFailTimes = 0

            tempSuccessTimes = 0
            successTimes = 0
            for valueLoc in range(len(tempSe)):
                if totalValue[valueLoc] >= 0:
                    tempSuccessTimes = tempSuccessTimes + 1
                else:
                    if tempSuccessTimes >= successTimes:
                        successTimes = tempSuccessTimes
                    tempSuccessTimes = 0

            resultSe = pd.Series([failTrade, successTrade, failTimes, successTimes],
                                 index=['负交易周', '正交易周', '最大连续上涨周数', '最大连续下跌周数'],
                                 name=tempSe.name)
            return resultSe

        fundPlotFormatDf = self.DateFormatDfDemo.getStrToDate(weekFundPlotDf)
        tempReturn = (fundPlotFormatDf - fundPlotFormatDf.shift(1)) / fundPlotFormatDf.shift(1)
        tempReturn.fillna(0, inplace=True)
        tradeResultDf = tempReturn.apply(upAndDownTrade)
        tradeResultDf.to_excel(resultPath + '周度胜率统计.xlsx')

    def plotWeekNetValueFigure(self, weekFundPlotDf, resultPath, fundName):
        '''
        周度收益相关统计与绘图
        :param weekFundPlotDf:
        :param resultPath:
        :param fundName:
        :return:
        '''
        fundPlotFormatDf = self.DateFormatDfDemo.getStrToDate(weekFundPlotDf)
        tempReturn = (fundPlotFormatDf - fundPlotFormatDf.shift(1)) / fundPlotFormatDf.shift(1)
        tempReturn.fillna(0, inplace=True)
        accReturn = (1 + tempReturn).cumprod() - 1

        plt.style.use('ggplot')
        fig1 = plt.figure(figsize=(16, 9))
        ax1 = fig1.add_subplot(111)
        accReturn.plot(ax=ax1)
        ax1.grid()
        ax1.set_xlabel('时间')
        ax1.set_ylabel('收益率')
        ax1.set_title('周度累计收益走势图')
        plt.savefig(resultPath + '周度累计收益走势图.png')

        plt.style.use('ggplot')
        fig2 = plt.figure(figsize=(16, 9))
        ax2 = fig2.add_subplot(111)
        tempSeUp = tempReturn[fundName].copy()
        tempSeUp[tempSeUp < 0] = np.nan
        tempSeUp.name = '正收益'
        tempSeDown = tempReturn[fundName].copy()
        tempSeDown[tempSeDown > 0] = np.nan
        tempSeDown.name = '负收益'
        tempDf = pd.concat([tempSeDown, tempSeUp], axis=1)
        tempDf.plot(kind='hist', ax=ax2, bins=20)
        ax2.grid()
        ax2.set_xlabel('周度收益率')
        ax2.set_ylabel('频率')
        ax2.set_title('周度收益率分布图')
        plt.savefig(resultPath + '周度收益率分布图.png')

        tempSe = tempReturn[fundName].copy()
        lossRate = len(tempSe[tempSe < 0]) / len(tempSe)
        successRate = 1 - lossRate
        tempPieSe = pd.Series([lossRate, successRate], index=['负交易周', '正交易周'], name='')
        plt.style.use('ggplot')
        fig3 = plt.figure(figsize=(16, 9))
        ax3 = fig3.add_subplot(111)
        ax3.set_title('周度交易胜负情况')
        tempPieSe.plot(kind='pie', autopct='%.2f%%', ax=ax3)
        plt.savefig(resultPath + '周度盈亏状况饼形图.png')

    def plotMonthNetValueFigure(self, monthFundPlotDf, resultPath, fundName):
        # fundPlotFormatDf = self.DateFormatDfDemo.getStrToDate(monthFundPlotDf)
        tempReturn = (monthFundPlotDf - monthFundPlotDf.shift(1)) / monthFundPlotDf.shift(1)
        tempReturn.fillna(0, inplace=True)
        plt.style.use('ggplot')
        fig1 = plt.figure(figsize=(16, 9))
        ax1 = fig1.add_subplot(111)
        tempReturn.plot(kind='bar', ax=ax1)
        ax1.grid()
        ax1.set_xlabel('时间')
        ax1.set_ylabel('收益率')
        ax1.set_title('月度收益率表现')
        plt.savefig(resultPath + '月度收益率表现图.png')
        # plt.show()

    def calcMonteCarlo(self,initValue, tradeDayList, mu, sigma,calcTimes=10000):
        '''
        蒙特卡洛算法
        :param initValue: 起始值
        :param days: 持有期
        :param mu: 收益率均值
        :param sigma: 收益率标准差
        :return:
        '''
        days = len(tradeDayList)
        dt = 1 / days
        dfList=[]
        for calcTime in range(calcTimes):
            price = np.zeros(days)
            price[0] = initValue
            # Schok and Drift
            shock = np.zeros(days)
            drift = np.zeros(days)

            # Run price array for number of days
            for x in range(1, days):
                # Calculate Schock
                shock[x] = np.random.normal(loc=mu*dt, scale=sigma * np.sqrt(dt))
                # Calculate Drift
                drift[x] = mu * dt
                # Calculate Price
                price[x] = price[x - 1] + (price[x - 1] * (drift[x] + shock[x]))
            dfList.append(pd.Series(price,index=tradeDayList))
        resultDf = pd.concat(dfList,axis=1)
        return resultDf

    def getMentoCaloForecast(self,fundPlotDf, resultPath, tradeDayList,fundName):
        fundPlotFormatDf = self.DateFormatDfDemo.getStrToDate(fundPlotDf)
        tempReturn = (fundPlotFormatDf - fundPlotFormatDf.shift(1)) / fundPlotFormatDf.shift(1)
        tempReturn.fillna(0, inplace=True)
        mu = tempReturn[fundName].mean()
        sigma = tempReturn[fundName].std()
        initValue = fundPlotDf.iloc[-1][fundName]

        resultDf = self.calcMonteCarlo(initValue=initValue,tradeDayList=tradeDayList,mu=mu,sigma=sigma)
        dicResult = {}
        dicResult['悲观'] = resultDf.quantile(0.25,axis=1)
        dicResult['中性'] = resultDf.quantile(0.5, axis=1)
        dicResult['乐观'] = resultDf.quantile(0.75, axis=1)
        forcastDf = pd.DataFrame(dicResult)

        dfDic = {}
        for colName in forcastDf.columns:
            tempInitSe = forcastDf[colName].copy()
            tempDf = pd.concat([fundPlotDf[fundName],tempInitSe],axis=0)
            tempDf = tempDf.drop_duplicates().sort_index()
            dfDic[colName] = tempDf
        dfDic[fundName] = fundPlotDf[fundName]
        resultFinalDf = pd.DataFrame(dfDic)
        resultFinalDf = self.DateFormatDfDemo.getStrToDate(resultFinalDf)

        plt.style.use('ggplot')
        fig1 = plt.figure(figsize=(16, 9))
        ax1 = fig1.add_subplot(111)
        resultFinalDf.plot(ax=ax1)
        ax1.grid()
        ax1.set_xlabel('时间')
        ax1.set_ylabel('收益率')
        ax1.set_title('模拟净值走势图')
        plt.savefig(resultPath + '模拟净值走势图.png')

        forcastLast = resultDf.iloc[-1]
        plt.style.use('ggplot')
        fig2 = plt.figure(figsize=(16, 9))
        ax2 = fig2.add_subplot(111)
        forcastLast.plot(ax=ax2,kind='hist',bins=50)
        ax2.grid()
        ax2.set_xlabel('时间')
        ax2.set_ylabel('频率')
        ax2.set_title('10000次模拟的三个月后净值变动频数分布图')
        plt.savefig(resultPath + '净值变动频数分布图.png')
        # plt.show()

        forcastRate = forcastLast/initValue-1
        condition1 = len(forcastRate[forcastRate<=-0.01])/len(forcastRate)
        condition2 = len(forcastRate[(forcastRate <= -0.005)&(-0.01<forcastRate)]) / len(forcastRate)
        condition3 = len(forcastRate[(forcastRate <= 0.0)&(-0.005<forcastRate)]) / len(forcastRate)
        condition4 = len(forcastRate[(forcastRate <= 0.005)&(0.0<forcastRate)]) / len(forcastRate)
        condition5 = len(forcastRate[(forcastRate <= 0.01)&(0.005<forcastRate)]) / len(forcastRate)
        condition6 = len(forcastRate[forcastRate > 0.01]) / len(forcastRate)
        indexList = ['<-1.0%','-1.0%~-0.5%','-0.5%~-0.0%','0.0%~0.5%','0.5%~1.0%','>1.0%']
        rateProSe = pd.Series([condition1,condition2,condition3,condition4,condition5,condition6],index=indexList,name='概率')
        rateProSe.index.name='收益率区间'
        rateProSe.to_excel(resultPath + '预测收益率概率表.xlsx')

if __name__ == '__main__':
    CalcRiskReturnDemo = CalcRiskReturn()
    CalcRiskReturn.calcRiskReturn()
