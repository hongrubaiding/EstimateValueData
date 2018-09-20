# -- coding: utf-8 --

'''
    主程序
'''

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import date, datetime
import matplotlib
from sqlalchemy import create_engine
from GetExcelData import GetExcelData
from BrisionAnys import BrisionAnys

class CalcEstimateValue:
    def __init__(self, fileTotalPath):
        self.fileTotalPath = fileTotalPath
        self.myfont = matplotlib.font_manager.FontProperties(fname=r'C:/Windows/Fonts/simkai.ttf')
        self.indexCodeList = ["000001.SH", "399300.SZ", "000016.SH", "000905.SH", "000906.SH"]

    # 数据处理并绘图
    def controlData(self, netAssetDf, dicProduct):
        fig = plt.figure(figsize=(16, 9))
        ax1 = fig.add_subplot(221)
        indexReturn = (netAssetDf[self.indexCodeList] - netAssetDf[self.indexCodeList].shift(1)) / netAssetDf[
            self.indexCodeList].shift(1)
        indexDfAcc = (1 + indexReturn).cumprod() - 1
        indexDfAcc.fillna(method='pad', inplace=True)
        df1 = pd.concat([indexDfAcc, netAssetDf['accNetReturn']], axis=1)
        df1.plot(ax=ax1)
        ax1.set_title(u'累计收益走势图', fontproperties=self.myfont)

        def historydownrate(tempdata):
            templist = []
            for k in range(len(tempdata)):
                downrate = tempdata[k] / tempdata[:k + 1].max() - 1
                templist.append(downrate)
            tempdf = pd.Series(templist, index=tempdata.index)
            tempdf.name = tempdata.name
            return tempdf

        downDf = netAssetDf[['netValue', '399300.SZ']].apply(historydownrate)
        ax2 = fig.add_subplot(222)
        downDf.plot(ax=ax2)
        ax2.set_title(u'回撤率走势图', fontproperties=self.myfont)

        ax3 = fig.add_subplot(223)
        netAssetDf['netReturn'].plot(ax=ax3, kind='hist', bins=20)
        ax3.set_title(u'收益率分布图', fontproperties=self.myfont)

        ax4 = fig.add_subplot(224)
        tempArr = netAssetDf['thisNetReturn'].values
        tempArrUp = len(tempArr[tempArr >= 0])
        upRate = tempArrUp / len(tempArr) * 100
        upRate = round(upRate, 2)
        downRate = 100 - upRate

        tempArrUpRate = str(upRate) + '%'
        tempArrDownRate = str(downRate) + '%'
        labels = [u'Trade + %s' % tempArrUpRate, u'Trade - %s' % tempArrDownRate]
        ax4.pie([upRate, downRate], labels=labels)
        ax4.set_title(u'盈亏状况统计图', fontproperties=self.myfont)
        plt.savefig('C:\\Users\\Administrator\\Desktop\\乐道4结果图\\' + '趋势图')

        fig2 = plt.figure(figsize=(16, 9))
        ax5 = fig2.add_subplot(211)
        netAssetDf['stockRate'].plot(ax=ax5, kind='bar', color='LightGreen')
        ax5.set_title(u'仓位变化图', fontproperties=self.myfont)
        ax5.set_ylabel(u'流通股票占比', fontproperties=self.myfont)

        # ax6 = ax5.twinx()
        # df1[['accNetReturn','399300.SZ']].plot(ax=ax6)
        # netAssetDf['accNetReturn'].plot(ax=ax6, color='red')
        # ax6.set_ylabel(u'累计净值增长率', fontproperties=self.myfont)

        ax7 = fig2.add_subplot(212)
        netAssetDf['annualStd'] = netAssetDf['netValue'].rolling(window=4).std() * np.sqrt(12)
        netAssetDf['annualStdHS300'] = indexReturn['399300.SZ'].rolling(window=4).std() * np.sqrt(12)
        netAssetDf[['annualStd', 'annualStdHS300']].dropna().plot(ax=ax7)
        ax7.set_title(u'滚动年化波动率走势图', fontproperties=self.myfont)
        ax7.set_ylabel(u'年化波动率', fontproperties=self.myfont)
        plt.savefig('C:\\Users\\Administrator\\Desktop\\乐道4结果图\\' + '波动变化图')

        fig3 = plt.figure(figsize=(16, 9))
        ax8 = fig3.add_subplot(211)
        tempDf = netAssetDf[
            ['cashRate', 'ensureMoneyRate', 'antiSaleRate', 'securityRate', 'fundRate', 'otherRate']].copy()
        tempDf.fillna(0, inplace=True)
        color = ['r', 'g', 'b', 'y', 'k', 'c', 'm']
        for i in range(tempDf.shape[1]):
            ax8.bar(tempDf.index.tolist(), tempDf.ix[:, i], color=color[i], bottom=tempDf.ix[:, :i].sum(axis=1),
                    width=3.95)
        ax8.set_label(['cashRate', 'ensureMoneyRate', 'antiSaleRate', 'securityRate', 'fundRate', 'otherRate'])
        for tick in ax8.get_xticklabels():
            tick.set_rotation(90)

        ax9 = fig3.add_subplot(212)
        tempdf = self.similateNet(netAssetDf[['netValue', 'netReturn','accNetReturn']])
        tempdf.plot(ax=ax9)
        plt.savefig('C:\\Users\\Administrator\\Desktop\\乐道4结果图\\' + '持仓分布图')
        plt.show()

    # 字符串拼接
    def CodeToStr(self, templist):
        tempstr = ''
        for temp in templist:
            tempstr = tempstr + "'" + temp + "'" + ","
        return tempstr[:-1]

    # 按照产品时间周期，获取指数历史数据
    def getIndexData(self, netAssetDf):
        startDate = netAssetDf.index.tolist()[0]
        endDate = netAssetDf.index.tolist()[-1]
        mysqlConfig = ['root', '123456', 'localhost', '3306', 'fund_data', 'utf8']
        mysqlcon = "mysql+pymysql://%s:%s@%s:%s/%s?charset=%s" % (
            mysqlConfig[0], mysqlConfig[1], mysqlConfig[2], mysqlConfig[3], mysqlConfig[4], mysqlConfig[5])
        conn = create_engine(mysqlcon)

        sqlStr = "select CODE,CLOSE,`UPDATE` from index_data where CODE in (%s) and `UPDATE`<='%s' and `UPDATE` >='%s'" % (
            self.CodeToStr(self.indexCodeList), endDate, startDate)
        tempDf1 = pd.read_sql(sql=sqlStr, con=conn)

        dflist = []
        for code, df in tempDf1.groupby(by=['CODE']):
            temp = pd.DataFrame(df['CLOSE'].values, index=df['UPDATE'].tolist(), columns=[code])
            dflist.append(temp)
        dflist.append(netAssetDf['netValue'])
        tempIndexDF = pd.concat(dflist, axis=1).fillna(method='pad')
        totalDf = pd.concat([netAssetDf, tempIndexDF[self.indexCodeList]], axis=1, join='inner')
        return totalDf

    # 模拟基金净值走势
    def similateNet(self, netDf):
        period = 6 * 4  # 预测周数
        rateMean = netDf['netReturn'].mean()
        rateStd = netDf['netReturn'].std()

        dicPredict = {}
        dicPredict['lowMarket'] = []
        dicPredict['highMarket'] = []
        dicPredict['middleMarket'] = []
        for i in range(period):
            randNum = np.random.normal(loc=rateMean, scale=rateStd, size=(100, 1))
            dicPredict['lowMarket'].append(np.percentile(randNum, 25, axis=0))
            dicPredict['middleMarket'].append(np.percentile(randNum, 50, axis=0))
            dicPredict['highMarket'].append(np.percentile(randNum, 75, axis=0))

        # lowMarket = netDf['netValue'][-1] * (1 + np.array(dicPredict['lowMarket']).cumsum())
        # highMarket = netDf['netValue'][-1] * (1 + np.array(dicPredict['highMarket']).cumsum())
        # middleMarket = netDf['netValue'][-1] * (1 + np.array(dicPredict['middleMarket']).cumsum())

        lowMarket = netDf['accNetReturn'][-1] * ((1 + np.array(dicPredict['lowMarket'])).cumprod())
        highMarket = netDf['accNetReturn'][-1] * ((1 + np.array(dicPredict['highMarket'])).cumprod())
        middleMarket = netDf['accNetReturn'][-1] * ((1 + np.array(dicPredict['middleMarket'])).cumprod())

        temp = list(pd.date_range(start=netDf.index.tolist()[-1], freq="W", periods=period))
        tempdate = [x.date() for x in temp]

        lowMarket = np.concatenate((netDf['accNetReturn'].values, np.array(lowMarket)))
        middleMarket = np.concatenate((netDf['accNetReturn'].values, np.array(middleMarket)))
        highMarket = np.concatenate((netDf['accNetReturn'].values, np.array(highMarket)))
        indexList = netDf.index.tolist()+tempdate

        df = pd.DataFrame(np.array([lowMarket, middleMarket, highMarket]).T, index=indexList,
                      columns=['lowMarket', 'middleMarket', 'highMarket'])
        return df

    def calcMain(self):
        netAssetDf, dicProduct = GetExcelData(self.fileTotalPath).getData()
        netAssetDf = self.getIndexData(netAssetDf)
        self.controlData(netAssetDf, dicProduct)

        # BrisionAnysDemo = BrisionAnys(dicProduct)
        # BrisionAnysDemo.calcMain()


if __name__ == '__main__':
    import os
    fileTotalPath = os.getcwd() + r'\乐道4估值表'  # 估值表文件夹路径
    CalcEstimateValueDemo = CalcEstimateValue(fileTotalPath=fileTotalPath)
    CalcEstimateValueDemo.calcMain()
