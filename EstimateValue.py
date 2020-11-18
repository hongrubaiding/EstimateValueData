# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

import pandas as pd
import numpy as np
from CalcRiskReturn import CalcRiskReturn
import os
from datetime import datetime, timedelta
from CalcRegression import CalcRegression
from FamaFrenchRegression import FamaFrenchRegression
from JudgeText import JudgeText
import mylog as mylog
from GetAndSaveWindData.GetDataTotalMain import GetDataTotalMain

import warnings

warnings.filterwarnings('ignore')


class EstimateValue:
    def __init__(self, dicParam):
        self.fundCode = dicParam['fundCode']
        self.netValuePeriod = dicParam.get('netValuePeriod', '')
        self.startDate = dicParam.get('startDate', '2014-06-17')
        endDate = datetime.today().strftime("%Y-%m-%d")
        self.endDate = dicParam.get('endDate', endDate)
        self.indexNameDic = {'000300.SH': '沪深300', '000852.SH': '中证1000', }
        # self.indexNameDic = {'000300.SH': '沪深300',}
        self.totalIndexName = list(self.indexNameDic.values())
        self.logger = mylog.set_log()
        self.GetDataTotalMainDemo = GetDataTotalMain(data_resource='wind')
        self.DCIndex = dicParam['DCIndex']

    def getNetValueDataDic(self,fundName):
        # indexCodeList = ['000300.SH', '000016.SH', '000905.SH', '000906.SH']
        indexCodeList = ['000300.SH']
        dicResult = {}
        self.logger.info("获取基金净值数据...")
        netValuedf = self.GetDataTotalMainDemo.get_hq_data(code=self.fundCode, start_date=self.startDate,
                                                      end_date=self.endDate, code_style='fund',
                                                      name_list=['net_value_adj'])
        self.logger.info("基金净值数据获取成功！")
        # self.fundName = netValuedf['fund_name'].unique()[0]
        self.fundName = fundName
        dicResult['fundName'] = fundName
        dicResult['netValuedf'] = netValuedf

        self.logger.info("获取大盘指数数据...")
        startDate = netValuedf.index.tolist()[0]
        endDate = netValuedf.index.tolist()[-1]

        dfIndexList = []
        dfVolumeList = []
        for indexCode in indexCodeList:
            indexDf = self.GetDataTotalMainDemo.get_hq_data(code=indexCode,start_date=startDate,end_date=endDate)
            indexDf.rename(columns={'close_price': indexCode}, inplace=True)
            dfIndexList.append(indexDf)

            indexDf = self.GetDataTotalMainDemo.get_hq_data(code=indexCode, start_date=startDate,
                                                                 end_date=endDate, name_list=['volume'])
            indexDf.rename(columns={'volume': indexCode}, inplace=True)
            dfVolumeList.append(indexDf)

        dicResult['DCIndexDf'] = pd.DataFrame()
        if self.DCIndex:
            DCIndexDf = self.GetDataTotalMainDemo.get_hq_data(code=self.DCIndex,start_date=startDate,end_date=endDate)
            DCIndexDf.rename(columns={'close_price': self.DCIndex}, inplace=True)
            dicResult['DCIndexDf'] = DCIndexDf

        self.logger.info("获取大盘指数数据成功！")
        totalIndexDf = pd.concat(dfIndexList, axis=1)
        totalVolumeDf = pd.concat(dfVolumeList, axis=1)
        dicResult['indexDf'] = totalIndexDf
        dicResult['totalVolumeDf'] = totalVolumeDf

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
        self.logger.info("获取申万一级行业指数数据...")
        for indexCode in industryList:
            industryDf = self.GetDataTotalMainDemo.get_hq_data(code=indexCode, start_date=startDate,
                                                                    end_date=endDate)
            industryDf.rename(columns={'close_price': indexCode}, inplace=True)
            dfIndestryList.append(industryDf)

        totalIndustryDf = pd.concat(dfIndestryList, axis=1)
        dicResult['totalIndustryDf'] = totalIndustryDf
        dicResult['industryDic'] = industryDic
        self.logger.info("获取申万一级行业指数数据成功！")

        # 风格指数
        styleList = ['801863.SI', '801822.SI', '801813.SI', '801831.SI', '801812.SI', '801821.SI', '801852.SI',
                     '801842.SI', '801843.SI', '801832.SI', '801851.SI', \
                     '801853.SI', '801841.SI', '801833.SI', '801823.SI', '801811.SI']
        styleLabel = ['新股指数', '中市盈率指数', '小盘指数', '高市净率指数', '中盘指数', '高市盈率指数', '微利股指数', '中价股指数', '低价股指数', '中市净率指数',
                      '亏损股指数', '绩优股指数', '高价股指数', '低市净率指数', '低市盈率指数', '大盘指数']
        styleDic = {sylteCode: styleName for sylteCode, styleName in zip(styleList, styleLabel)}
        dfStyleList = []
        self.logger.info("获取风格指数数据...")
        for indexCode in styleList:
            styleDf = self.GetDataTotalMainDemo.get_hq_data(code=indexCode, start_date=startDate,
                                             end_date=endDate)
            styleDf.rename(columns={'close_price': indexCode}, inplace=True)
            dfStyleList.append(styleDf)
        totalStyleDf = pd.concat(dfStyleList, axis=1)
        dicResult['totalStyleDf'] = totalStyleDf
        dicResult['styleDic'] = styleDic
        self.logger.info("获取风格指数数据成功")
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
        fundIndexDf = pd.concat([dicNetValueResult['netValuedf']['net_value_adj'], dicNetValueResult['indexDf']],
                                axis=1, join='inner')
        fundIndexDf.rename(columns={'net_value_adj': dicNetValueResult['fundName']}, inplace=True)
        fundPlotDf = fundIndexDf.rename(columns=self.indexNameDic)

        CalcRiskReturnDemo = CalcRiskReturn()
        self.logger.info("计算日频数据相关结论...")
        CalcRiskReturnDemo.calcRiskReturn(fundPlotDf, resultPath)
        marketVolume = dicNetValueResult['totalVolumeDf']
        CalcRiskReturnDemo.plotDayNetValueFigure(fundPlotDf, resultPath, fundName=self.fundName,
                                                 netPeriod=self.netValuePeriod, marketVolume=marketVolume)

        startDate = fundPlotDf.index.tolist()[-1]
        startDate = datetime.strptime(startDate, "%Y-%m-%d")
        endDate = startDate + timedelta(days=31 * 3)

        tradeDayList = self.GetDataTotalMainDemo.get_tradeday(start_date=startDate,end_date=endDate,period=self.netValuePeriod)
        CalcRiskReturnDemo.getMentoCaloForecast(fundPlotDf, resultPath, tradeDayList, fundName=self.fundName)

        self.logger.info("计算周频数据相关结论...")
        tradeWeekList = self.GetDataTotalMainDemo.get_tradeday(start_date=fundPlotDf.index.tolist()[0], end_date=fundPlotDf.index.tolist()[-1],
                                                              period='W')
        weekFundPlotDf = fundPlotDf.loc[tradeWeekList].dropna(axis=0)
        CalcRiskReturnDemo.plotWeekNetValueFigure(weekFundPlotDf, resultPath, fundName=self.fundName)
        CalcRiskReturnDemo.calcWeekNetValueResult(weekFundPlotDf, resultPath, fundName=self.fundName)

        self.logger.info("计算月频数据相关结论...")
        tradeMonthList = self.GetDataTotalMainDemo.get_tradeday(start_date=fundPlotDf.index.tolist()[0], end_date=fundPlotDf.index.tolist()[-1],
                                                              period='M')
        monthFundPlotDf = fundPlotDf.loc[tradeMonthList].dropna(axis=0)
        CalcRiskReturnDemo.plotMonthNetValueFigure(monthFundPlotDf, resultPath, fundName=self.fundName)

        targetDf = fundPlotDf.copy()
        targetDf['无风险利率'] = self.getRiskFree()
        CalcRegressionDemo = CalcRegression()
        self.logger.info("计算选股，择时能力相关结论...")


        CalcRegressionDemo.getSelectStockAndTime(targetDf, resultPath, fundName=self.fundName,
                                                 netPeriod=self.netValuePeriod, benchMark=list(self.indexNameDic.values())[0],DCIndexDf=dicNetValueResult['DCIndexDf'])

        self.logger.info("计算行业，风格回归相关结论...")
        fundIndustryDf = pd.concat(
            [dicNetValueResult['netValuedf']['net_value_adj'], dicNetValueResult['totalIndustryDf']],
            axis=1, join='inner')
        fundIndustryDf.rename(columns={'net_value_adj': dicNetValueResult['fundName']}, inplace=True)
        fundIndustryDf['无风险利率'] = self.getRiskFree()
        CalcRegressionDemo.getIndustryRegression(fundIndustryDf, resultPath, fundName=self.fundName,
                                                 industryDic=dicNetValueResult['industryDic'],bench_return=dicNetValueResult['indexDf'])

        fundIndustryDf = pd.concat(
            [dicNetValueResult['netValuedf']['net_value_adj'], dicNetValueResult['totalStyleDf']],
            axis=1, join='inner')
        fundIndustryDf.rename(columns={'net_value_adj': dicNetValueResult['fundName']}, inplace=True)
        fundIndustryDf['无风险利率'] = self.getRiskFree()
        CalcRegressionDemo.getStyleRegression(fundIndustryDf, resultPath, fundName=self.fundName,
                                              industryDic=dicNetValueResult['styleDic'],DCIndexDf=dicNetValueResult['DCIndexDf'])

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

    def getMain(self,fundName='缺基金名称'):
        dicNetValueResult = self.getNetValueDataDic(fundName=fundName)  # 获取产品净值数据和指数数据
        resultPath = self.getSavePath()  # 创建分析结果保存文件路径
        #
        # FamaFrenchRegressionDemo = FamaFrenchRegression()
        # FamaFrenchRegressionDemo.calcResult(resultPath,dicNetValueResult['totalIndustryDf'],dicNetValueResult['industryDic'])
        # FamaFrenchRegressionDemo.calcMain(closePriceSe=dicNetValueResult['netValuedf']['net_value_adj'],resultPath=resultPath)

        self.calcAndPlotSaveRiskReturn(dicNetValueResult, resultPath)  # 净值类统计结果，按统计周期分析与保存
        JudgeTextDemo = JudgeText()
        JudgeTextDemo.getNetJudgeText(fundCode=self.fundCode, fundName=self.fundName,
                                      totalIndexName=self.totalIndexName)
        self.logger.info("计算完成！")


if __name__ == '__main__':
    # 乐道S60034  宽远S35529,000409.OF

    nameDic = {'费曼一号（增强IE500）': 'SEP131', '华量锐天1号（T0对冲IE500）': 'SW7742', '阿尔法对冲': 'SK7720'}
    # codeList = ['SS2221', 'SY3702']
    codeList = ['519062.OF']

    for fundcode in codeList:
        print(fundcode)
        dicParam = {}
        dicParam['fundCode'] = fundcode  # 基金代码
        dicParam['netValuePeriod'] = 'D'  # 净值披露频率
        dicParam['startDate'] = '2014-11-30'
        dicParam['DCIndex'] = ''           #对冲类产品，默认为空；非空时为对冲的指数代码

        EstimateValueDemo = EstimateValue(dicParam=dicParam)
        EstimateValueDemo.getMain(fundName='海富通阿尔法对冲')
