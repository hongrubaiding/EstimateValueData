# -- coding: utf-8 --

'''
    多期Brisio业绩归因分析
'''

import pandas as pd
import numpy as np
from WindPy import w
import matplotlib.pyplot as plt
import matplotlib


class BrisionAnys:
    def __init__(self, dicStockDf):
        self.benchMark = '000300.SH'  # 基准指数
        self.dicStockDf = self.WashData(dicStockDf)
        self.myfont = matplotlib.font_manager.FontProperties(fname=r'C:/Windows/Fonts/simkai.ttf')

        # self.indusFieds = ['INDUSTRY_GICS', 'INDUSTRY_GICSCODE']        #wind行业名称和代码
        # self.indusFieds = ['INDUSTRY_CSRCCODE12', 'INDUSTRY_CSRC12']    #证监会行业名称和代码
        # self.indusFieds = ['INDUSTRY_SW', 'INDUSTRY_SWCODE']  # 申万行业名称和代码
        self.indusFieds = ['INDUSTRY_CITIC', 'INDUSTRY_CITICCODE']  # 中信行业名称和代码

    # 数据清洗，完善后可移动至其他模块
    def WashData(self, dicStockDf):
        dicTotalStock = {}
        for stockDate, stockDf in dicStockDf.items():
            stockList = [stock.replace(' ', '.') for stock in stockDf.index.tolist()]
            tempDf = pd.DataFrame(stockDf.values, index=stockList,
                                  columns=['styleCode', 'stockName', 'styleMoney', 'forRate', 'stockNum',
                                           'stockUnitBuy',
                                           'stockTotalBuy', 'stockBuyWeight0', 'stockClosePrice', 'stockValue',
                                           'stockValueWeight', 'stockChange', 'stockTradeFlag'])
            dicTotalStock[stockDate] = tempDf
        return dicTotalStock

    # 计算组合收益部分
    def calcPoforlio(self, tradeNum, tradeProcess):
        tradeDate = tradeProcess[tradeNum][0]  # 当前交易日期
        stockDf = tradeProcess[tradeNum][1]  # 当前持仓个股

        stockList = stockDf.index.tolist()
        wsdata = w.wss(codes=stockList, fields=self.indusFieds,
                       options="industryType=1;tradeDate=%s" % tradeDate)
        if wsdata.ErrorCode != 0:
            return
        tempDf = pd.DataFrame(wsdata.Data, columns=wsdata.Codes, index=wsdata.Fields).T
        stockDf1 = pd.concat([stockDf, tempDf], join='inner', axis=1)
        try:
            cht = stockDf['stockClosePrice'] / stockDf1['stockUnitBuy']
        except:
            cht= pd.Series([1]*stockDf1.shape[0],index=stockDf1.index)                     #打新股时，无行情
        stockDf1['logReturn'] = np.log(cht.tolist())
        stockDf1['iWeight'] = stockDf1['stockBuyWeight0'] / stockDf1['stockBuyWeight0'].sum()

        dicIndustry = {}
        for industryCode, tempDf in stockDf1.groupby(by=self.indusFieds[0]):
            tempDf['weightReturn'] = tempDf.loc[:,'logReturn'] * tempDf.loc[:,'iWeight']
            industryName = list(tempDf[self.indusFieds[1]].unique())[0]
            dicIndustry[industryCode] = {'stockReturn': tempDf['weightReturn'].sum(), 'stockINName': industryName,
                                         'stockWeight': tempDf['iWeight'].sum()}
        stockDf2 = pd.DataFrame(dicIndustry).T
        return stockDf2

    # 计算基准收益部分
    def calcBenchMark(self, tradeNum, tradeProcess):
        tradeDate = tradeProcess[tradeNum][0]  # 当前交易日期

        # 指数成分股
        indexWSData = w.wset("indexconstituent", "date=%s;windcode=%s" % (tradeDate, self.benchMark))
        if indexWSData.ErrorCode != 0:
            return
        indexData = pd.DataFrame(indexWSData.Data, index=['checkDate', 'windCode', 'windName', 'iWeight']).T
        indexData.set_index(keys='windCode', inplace=True)

        # 成股份对应的wind行业
        indexCodeList = indexData.index.tolist()
        wsindexdata = w.wss(codes=indexCodeList, fields=self.indusFieds,
                            options="industryType=1;tradeDate=%s" % tradeDate)
        if wsindexdata.ErrorCode != 0:
            return
        tempIndexDf = pd.DataFrame(wsindexdata.Data, columns=wsindexdata.Codes, index=wsindexdata.Fields).T

        # 成分股收盘行情数据
        indexClose = w.wss(codes=tempIndexDf.index.tolist(), fields=['close'],
                           options="tradeDate=%s;cycle=D;priceAdj=F" % tradeDate)
        indexCloseDf = pd.DataFrame(indexClose.Data, index=['close'], columns=indexClose.Codes).T

        beforeDate = tradeProcess[tradeNum - 1][0]  # 上一净值发布日期
        indexClose = w.wss(codes=tempIndexDf.index.tolist(), fields=['close'],
                           options="tradeDate=%s;cycle=D;priceAdj=F" % beforeDate)
        indexBeforeCloseDf = pd.DataFrame(indexClose.Data, index=['beforeClose'], columns=indexClose.Codes).T
        indexDF = pd.concat([indexData, tempIndexDf, indexBeforeCloseDf, indexCloseDf], axis=1, join='inner')
        indexDF['logReturn'] = np.log(indexDF['close'] / indexDF['beforeClose'])
        indexDF['iWeight'] = indexDF['iWeight'] / indexDF['iWeight'].sum()

        dicIndustry = {}
        for industryCode, tempDf in indexDF.groupby(by=[self.indusFieds[0]]):
            tempDf['weightReturn'] = tempDf['logReturn'] * tempDf['iWeight']
            industryName = list(tempDf[self.indusFieds[1]].unique())[0]
            dicIndustry[industryCode] = {'indexReturn': tempDf['weightReturn'].sum(), 'indexINName': industryName,
                                         'indexWeight': tempDf['iWeight'].sum()}

        industryDf = pd.DataFrame(dicIndustry).T
        return industryDf

    # 计算入口
    def calc(self, dicTotalStock):
        w.start()
        tradeProcess = sorted(dicTotalStock.items(), key=lambda x: x[0], reverse=False)
        dicReturnFactor = {}
        dicReturnFactor['SR'] = []
        dicReturnFactor['AR'] = []
        dicReturnFactor['IR'] = []
        dicReturnFactor['pofolioReturn'] = []
        dicReturnFactor['benchMarkReturn'] = []
        dicReturnFactor['everySR'] = []
        dicReturnFactor['everyAR'] = []
        dicReturnFactor['everyIR'] = []

        for tradeNum in range(1, len(tradeProcess)):
        # for tradeNum in range(1, 5):
            tradeDate = tradeProcess[tradeNum][0]  # 当前交易日期
            print('当前交易日期：', tradeDate)

            # 投资组合所属行业和收益
            stockDf = self.calcPoforlio(tradeNum, tradeProcess)

            # 指数成分股的所属行业和收益
            indexDf = self.calcBenchMark(tradeNum, tradeProcess)

            totaldf = pd.concat([indexDf, stockDf], axis=1, join='outer')
            totaldf.fillna(0, inplace=True)
            pofolioReturn = (totaldf['stockReturn'] * totaldf['stockWeight']).sum()
            benchMarkReturn = (totaldf['indexReturn'] * totaldf['indexWeight']).sum()
            kFactor = (np.log(1 + pofolioReturn) - np.log(1 + benchMarkReturn)) / (pofolioReturn - benchMarkReturn)

            dicReturnFactor['industryName'] = dicReturnFactor.get('industryName',totaldf.index.tolist())
            everySR = kFactor * ((totaldf['stockReturn'] - totaldf['indexReturn']) * totaldf['indexWeight'])
            everyAR = kFactor * ((totaldf['stockWeight'] - totaldf['indexWeight']) * totaldf['indexReturn'])
            everyIR = kFactor * ((totaldf['stockWeight'] - totaldf['indexWeight']) * (
                        totaldf['stockReturn'] - totaldf['indexReturn']))
            dicReturnFactor['everySR'].append(everySR)
            dicReturnFactor['everyAR'].append(everyAR)
            dicReturnFactor['everyIR'].append(everyIR)

            SR = kFactor * ((totaldf['stockReturn'] - totaldf['indexReturn']) * totaldf['indexWeight']).sum()
            AR = kFactor * ((totaldf['stockWeight'] - totaldf['indexWeight']) * totaldf['indexReturn']).sum()
            IR = kFactor * ((totaldf['stockWeight'] - totaldf['indexWeight']) * (
                        totaldf['stockReturn'] - totaldf['indexReturn'])).sum()
            dicReturnFactor['SR'].append(SR)
            dicReturnFactor['AR'].append(AR)
            dicReturnFactor['IR'].append(IR)
            dicReturnFactor['pofolioReturn'].append(pofolioReturn)
            dicReturnFactor['benchMarkReturn'].append(benchMarkReturn)
        return dicReturnFactor

    # 计算同时期基准总收益的分解
    def totalTradeAny(self, dicReturnFactor):
        result = {}
        npSR = np.array(dicReturnFactor['SR'])
        npAR = np.array(dicReturnFactor['AR'])
        npIR = np.array(dicReturnFactor['IR'])

        nppofolioReturn = np.array(dicReturnFactor['pofolioReturn'])
        npbenchMarkReturn = np.array(dicReturnFactor['benchMarkReturn'])
        totalPofolio = (1 + nppofolioReturn).cumprod() - 1
        totalBenchMark = (1 + npbenchMarkReturn).cumprod() - 1
        totalKFactor =(np.log(1+totalPofolio[-1]) - np.log(1+totalBenchMark[-1]))/(totalPofolio[-1]-totalBenchMark[-1])
        dicReturnFactor['totalKFactor'] = totalKFactor

        result['SR'] = npSR.sum() /totalKFactor
        result['AR'] = npAR.sum() / totalKFactor
        result['IR'] = npIR.sum() / totalKFactor
        return result

    #绘图入口
    def plotFigure(self,dicReturnFactor):
        plt.rcParams['font.sans-serif'] = ['SimHei']
        plt.rcParams['axes.unicode_minus'] = False

        everySR = np.array(dicReturnFactor['everySR'])
        totalSR = np.sum(everySR, axis=0)/dicReturnFactor['totalKFactor']
        SRSeries = pd.Series(totalSR, index=dicReturnFactor['industryName'])

        everyAR = np.array(dicReturnFactor['everyAR'])
        totalAR = np.sum(everyAR, axis=0) / dicReturnFactor['totalKFactor']
        ARSeries = pd.Series(totalAR, index=dicReturnFactor['industryName'])

        everyIR = np.array(dicReturnFactor['everyIR'])
        totalIR = np.sum(everyIR, axis=0) / dicReturnFactor['totalKFactor']
        IRSeries = pd.Series(totalIR, index=dicReturnFactor['industryName'])

        fig = plt.figure(figsize=(16, 9))
        ax1 = fig.add_subplot(131)
        ARSeries = ARSeries.sort_values()
        ARSeries.plot(kind='barh', ax=ax1)
        ax1.set_title(u'行业配置收益')

        ax2 = fig.add_subplot(132)
        SRSeries = SRSeries.sort_values()
        SRSeries.plot(kind='barh', ax=ax2)
        ax2.set_title(u'个股选择收益')

        ax3 = fig.add_subplot(133)
        IRSeries = IRSeries.sort_values()
        IRSeries.plot(kind='barh', ax=ax3)
        ax3.set_title(u'交互收益')
        plt.savefig('C:\\Users\\Administrator\\Desktop\\乐道4结果图\\' + '归因分析')
        plt.show()

    # 总入口
    def calcMain(self):
        dicReturnFactor = self.calc(self.dicStockDf)
        result = self.totalTradeAny(dicReturnFactor)
        self.plotFigure(dicReturnFactor)
        print(result)


if __name__ == '__main__':
    import os
    from GetExcelData import GetExcelData

    fileTotalPath = os.getcwd() + r'\乐道4估值表'  # 估值表文件夹路径
    netAssetDf, dicProduct = GetExcelData(fileTotalPath).getData()
    BrisionAnysDemo = BrisionAnys(dicProduct)
    BrisionAnysDemo.calcMain()
