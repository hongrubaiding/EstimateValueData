# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

import pandas as pd
import numpy as np
from PrintInfo import PrintInfo
from DataToMySql.MysqlCon import MysqlCon
from GetDataFromWindAndMySql import GetDataFromWindAndMySql
from CalcRiskReturn import CalcRiskReturn
import os
from datetime import datetime, timedelta
from CalcRegression import CalcRegression
from FamaFrenchRegression import FamaFrenchRegression
from JudgeText import JudgeText
import warnings
warnings.filterwarnings('ignore')


class EstimateValue:
    def __init__(self, dicParam):
        self.fundCode = dicParam['fundCode']
        self.netValuePeriod = dicParam.get('netValuePeriod', '')
        self.isPosition = dicParam.get('isPosition', False)
        self.startDate = dicParam.get('startDate', '2015-01-01')
        self.endDate = dicParam.get('endDate', '2019-01-01')
        self.indexNameDic = {'000300.SH': '沪深300', '000016.SH': '上证50', '000905.SH': '中证500', '000906.SH': '中证800'}
        self.totalIndexName = list(self.indexNameDic.values())

        self.engine = MysqlCon().getMysqlCon()
        self.PrintInfoDemo = PrintInfo()
        self.GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()

    def getNetValueDataDic(self):
        indexCodeList = ['000300.SH', '000016.SH', '000905.SH', '000906.SH']
        dicResult = {}
        self.PrintInfoDemo.PrintLog("获取基金净值数据...")
        sqlStr = "select fund_name,update_time,net_value,acc_net_value " \
                 "from fund_net_value where fund_code='%s'" % self.fundCode
        netValuedf = pd.read_sql(sql=sqlStr, con=self.engine, index_col='update_time')
        if netValuedf.empty:
            netValuedf = self.GetDataFromWindAndMySqlDemo.getHQData(tempCode=self.fundCode, startDate=self.startDate,
                                                                 endDate=self.endDate,tableFlag='fund',nameList=['fund_name','acc_net_value'])
        self.PrintInfoDemo.PrintLog("基金净值数据获取成功！")
        self.fundName = netValuedf['fund_name'].unique()[0]
        dicResult['fundName'] = self.fundName
        netValuedf.drop(labels='fund_name', inplace=True, axis=1)
        dicResult['netValuedf'] = netValuedf

        self.PrintInfoDemo.PrintLog("获取大盘指数数据...")
        startDate = netValuedf.index.tolist()[0]
        endDate = netValuedf.index.tolist()[-1]

        dfIndexList = []
        for indexCode in indexCodeList:
            indexDf = self.GetDataFromWindAndMySqlDemo.getHQData(tempCode=indexCode, startDate=startDate,
                                                                 endDate=endDate)
            indexDf.rename(columns={'close_price': indexCode}, inplace=True)
            dfIndexList.append(indexDf)

        self.PrintInfoDemo.PrintLog("获取大盘指数数据成功！")
        totalIndexDf = pd.concat(dfIndexList, axis=1)
        dicResult['indexDf'] = totalIndexDf

        # 行业指数
        industryList = ['801210.SI', '801050.SI', '801140.SI', '801020.SI', '801170.SI', '801030.SI', '801150.SI',
                        '801010.SI', '801200.SI', '801230.SI', '801770.SI', '801730.SI', \
                        '801130.SI', '801880.SI', '801180.SI', '801160.SI', '801780.SI', '801890.SI', '801080.SI',
                        '801760.SI', '801790.SI', '801710.SI', '801740.SI', '801720.SI', \
                        '801750.SI', '801110.SI', '801040.SI', '801120.SI']
        industryLabel = ['休闲服务', '有色金属', '轻工制造', '采掘', '交通运输', '化工', '医药生物', '农林牧渔', '商业贸易', '综合', '通信', '电气设备', '纺织服装',
                         '汽车', '房地产', '公用事业', \
                         '银行', '机械设备', '电子', '传媒', '非银金融', '建筑材料', '国防军工', '建筑装饰', '计算机', '家用电器', '钢铁', '食品饮料']
        industryDic = {industryCode: industryName for industryCode, industryName in zip(industryList, industryLabel)}
        dfIndestryList = []
        self.PrintInfoDemo.PrintLog("获取申万一级行业指数数据...")
        for indexCode in industryList:
            industryDf = self.GetDataFromWindAndMySqlDemo.getHQData(tempCode=indexCode, startDate=startDate,
                                                                    endDate=endDate)
            industryDf.rename(columns={'close_price': indexCode}, inplace=True)
            dfIndestryList.append(industryDf)

        totalIndustryDf = pd.concat(dfIndestryList, axis=1)
        dicResult['totalIndustryDf'] = totalIndustryDf
        dicResult['industryDic'] = industryDic
        self.PrintInfoDemo.PrintLog("获取申万一级行业指数数据成功！")

        # 风格指数
        styleList = ['801863.SI', '801822.SI', '801813.SI', '801831.SI', '801812.SI', '801821.SI', '801852.SI',
                      '801842.SI', '801843.SI', '801832.SI', '801851.SI', \
                      '801853.SI', '801841.SI', '801833.SI', '801823.SI', '801811.SI']
        styleLabel = ['新股指数', '中市盈率指数', '小盘指数', '高市净率指数', '中盘指数', '高市盈率指数', '微利股指数', '中价股指数', '低价股指数', '中市净率指数',
                       '亏损股指数', '绩优股指数', '高价股指数', '低市净率指数', '低市盈率指数', '大盘指数']
        styleDic = {sylteCode: styleName for sylteCode, styleName in zip(styleList, styleLabel)}
        dfStyleList = []
        self.PrintInfoDemo.PrintLog("获取风格指数数据...")
        for indexCode in styleList:
            styleDf = self.GetDataFromWindAndMySqlDemo.getHQData(tempCode=indexCode, startDate=startDate,
                                                                    endDate=endDate)
            styleDf.rename(columns={'close_price': indexCode}, inplace=True)
            dfStyleList.append(styleDf)
        totalStyleDf = pd.concat(dfStyleList, axis=1)
        dicResult['totalStyleDf'] = totalStyleDf
        dicResult['styleDic'] = styleDic
        self.PrintInfoDemo.PrintLog("获取风格指数数据成功")
        return dicResult

    def getRiskFree(self):
        if self.netValuePeriod == 'W':
            riskFree = 0.02 / 52
        else:
            riskFree = 0.02 / 250
        return riskFree

    def calcAndPlotSaveRiskReturn(self, dicNetValueResult, resultPath):
        '''
            计算并保存指定周期的风险收益指标
            绘图
        :param dicNetValueResult:
        :return:
        '''

        fundIndexDf = pd.concat([dicNetValueResult['netValuedf']['acc_net_value'], dicNetValueResult['indexDf']],
                                axis=1, join='inner')
        fundIndexDf.rename(columns={'acc_net_value': dicNetValueResult['fundName']}, inplace=True)
        fundPlotDf = fundIndexDf.rename(columns=self.indexNameDic)

        CalcRiskReturnDemo = CalcRiskReturn()
        self.PrintInfoDemo.PrintLog("计算日频数据相关结论...")
        CalcRiskReturnDemo.calcRiskReturn(fundPlotDf, resultPath)
        CalcRiskReturnDemo.plotDayNetValueFigure(fundPlotDf, resultPath, fundName=self.fundName,
                                                 netPeriod=self.netValuePeriod)

        startDate = fundPlotDf.index.tolist()[-1]
        startDate = datetime.strptime(startDate, "%Y-%m-%d")
        endDate = startDate + timedelta(days=31 * 3)
        tradeDayList = self.GetDataFromWindAndMySqlDemo.getTradeDay(startdate=startDate, endDate=endDate,
                                                                    Period=self.netValuePeriod)
        CalcRiskReturnDemo.getMentoCaloForecast(fundPlotDf, resultPath, tradeDayList, fundName=self.fundName)

        self.PrintInfoDemo.PrintLog("计算周频数据相关结论...")
        tradeWeekList = self.GetDataFromWindAndMySqlDemo.getTradeDay(startdate=fundPlotDf.index.tolist()[0],
                                                                     endDate=fundPlotDf.index.tolist()[-1], Period='W')
        weekFundPlotDf = fundPlotDf.loc[tradeWeekList].dropna(axis=0)
        CalcRiskReturnDemo.plotWeekNetValueFigure(weekFundPlotDf, resultPath, fundName=self.fundName)
        CalcRiskReturnDemo.calcWeekNetValueResult(weekFundPlotDf, resultPath, fundName=self.fundName)

        self.PrintInfoDemo.PrintLog("计算月频数据相关结论...")
        tradeMonthList = self.GetDataFromWindAndMySqlDemo.getTradeDay(startdate=fundPlotDf.index.tolist()[0],
                                                                      endDate=fundPlotDf.index.tolist()[-1], Period='M')
        monthFundPlotDf = fundPlotDf.loc[tradeMonthList].dropna(axis=0)
        CalcRiskReturnDemo.plotMonthNetValueFigure(monthFundPlotDf, resultPath, fundName=self.fundName)

        targetDf = fundPlotDf.copy()
        targetDf['无风险利率'] = self.getRiskFree()
        CalcRegressionDemo = CalcRegression()
        self.PrintInfoDemo.PrintLog("计算选股，择时能力相关结论...")
        CalcRegressionDemo.getSelectStockAndTime(targetDf, resultPath, fundName=self.fundName,
                                                 netPeriod=self.netValuePeriod, benchMark='沪深300')

        self.PrintInfoDemo.PrintLog("计算行业，风格回归相关结论...")
        fundIndustryDf = pd.concat(
            [dicNetValueResult['netValuedf']['acc_net_value'], dicNetValueResult['totalIndustryDf']],
            axis=1, join='inner')
        fundIndustryDf.rename(columns={'acc_net_value': dicNetValueResult['fundName']}, inplace=True)
        fundIndustryDf['无风险利率'] = self.getRiskFree()
        CalcRegressionDemo.getIndustryRegression(fundIndustryDf, resultPath, fundName=self.fundName,
                                                 industryDic=dicNetValueResult['industryDic'])

        fundIndustryDf = pd.concat(
            [dicNetValueResult['netValuedf']['acc_net_value'], dicNetValueResult['totalStyleDf']],
            axis=1, join='inner')
        fundIndustryDf.rename(columns={'acc_net_value': dicNetValueResult['fundName']}, inplace=True)
        fundIndustryDf['无风险利率'] = self.getRiskFree()
        CalcRegressionDemo.getStyleRegression(fundIndustryDf, resultPath, fundName=self.fundName,
                                              industryDic=dicNetValueResult['styleDic'])

    def getSavePath(self):
        '''
        获取保存产品分析结果的路径
        :return:
        '''
        totalFileList = os.listdir(os.getcwd() + r"\\分析结果\\")
        if self.fundName not in totalFileList:
            os.mkdir(path=os.getcwd() + r"\\分析结果\\%s\\" % self.fundName)
        resultPath = os.getcwd() + r"\\分析结果\\%s\\" % self.fundName
        return resultPath

    def getMain(self):
        dicNetValueResult = self.getNetValueDataDic()  # 获取产品净值数据和指数数据
        resultPath = self.getSavePath()                 #创建分析结果保存文件路径
        #
        FamaFrenchRegressionDemo = FamaFrenchRegression()
        FamaFrenchRegressionDemo.calcMain(closePriceSe=dicNetValueResult['netValuedf']['acc_net_value'],resultPath=resultPath)

        # self.calcAndPlotSaveRiskReturn(dicNetValueResult, resultPath)  # 净值类统计结果，按统计周期分析与保存
        # JudgeTextDemo = JudgeText()
        # JudgeTextDemo.getNetJudgeText(fundCode=self.fundCode,fundName=self.fundName,totalIndexName=self.totalIndexName)


if __name__ == '__main__':
    # 乐道S60034  宽远S35529,000409.OF
    chenYuProduct = {}
    chenYuProduct['锐阳6号'] = 'SCZ679'
    chenYuProduct['金牛3号'] = 'SW5068'
    chenYuProduct['金牛5号'] = 'SY7337'

    dicParam = {}
    dicParam['fundCode'] = '160630.OF'  # 基金代码
    dicParam['netValuePeriod'] = ''  # 净值披露频率
    dicParam['isPosition'] = False
    dicParam['startDate'] = '2015-01-01'
    dicParam['endDate'] = '2016-01-01'

    EstimateValueDemo = EstimateValue(dicParam=dicParam)
    EstimateValueDemo.getMain()
