# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com
'''
    基于分析结果，给出标准化输出评价
'''

import pandas as pd
import os


class JudgeText:
    def __init__(self, ):
        pass

    def getCompareIndex(self, tempDic, fundName, compareName, formatFlag=True, rightFlag=True):
        '''
        同期市场指数相比
        :param tempDic:
        :param fundName:
        :param compareName: 对比的指标名称
        :param formatFlag: 是否对取值化为百分比，保留2为有效数字
        :param rightFlag: 是否是取值越大越好
        :return:
        '''
        indexTradeStr = ''
        goodTrade = []
        badTrade = []
        for name, value in tempDic.items():
            if name != fundName:
                if formatFlag:
                    indexTradeStr = indexTradeStr + name + ',%.2f%%' % (value * 100) + ','
                else:
                    indexTradeStr = indexTradeStr + name + ',%.2f' % (value) + ','
                if value < tempDic[fundName]:
                    goodTrade.append(name)
                else:
                    badTrade.append(name)

        if formatFlag:
            resultText = "该产品的%s为%.2f%%" % (compareName, tempDic[fundName] * 100) + ',同期市场变现为%s' % (indexTradeStr)
        else:
            resultText = "该产品的%s为%.2f" % (compareName, tempDic[fundName]) + ',同期市场变现为%s' % (indexTradeStr)

        if rightFlag:
            if goodTrade:
                resultText = resultText + '即，强于%s' % (','.join(goodTrade))

            if badTrade:
                resultText = resultText + '较弱与%s' % (','.join(badTrade))
        else:
            if badTrade:
                resultText = resultText + '即，强于%s' % (','.join(badTrade))

            if goodTrade:
                resultText = resultText + '较弱与%s' % (','.join(goodTrade))
        return resultText

    def getNetJudgeText(self, totalIndexName, fundName='华夏大盘精选', fundCode='000011.OF'):
        resultPath = os.getcwd() + r"\\分析结果\\%s\\" % fundName
        gaiKuoTxt = "基金绩效评价在证券分析是重要的一部分。绩效评价系统性地可分为三个部分：最基本的绩效衡量，" \
                    "深层次的绩效归因，以及最终成熟的绩效评价。\n本文选择%s(%s)为基金研究对象，" \
                    "市场组合用%s来近似，对该基金在期间的绩" \
                    "效进行评估。\n全文分为5个部分：基金绩效衡量概况，在这里最基金绩效进行基本指标的度量；选股择时能力，" \
                    "利用CAPM的衍生模型来度量该基金的选股择时能力；\n多因子归因，从多个因子的角度来做基金收益率做归因；" \
                    "行业归因；风格归因。" % (fundName, fundCode, '、'.join(totalIndexName)) + '\n\n'
        riskReturnText = "风险收益统计指标结果：\n"

        weekSuccessDf = pd.read_excel(resultPath + "周度胜率统计.xlsx", index_col=0)
        trandSuccess = weekSuccessDf.loc['正交易周'].to_dict()
        weekSuccessText = self.getCompareIndex(trandSuccess, fundName, compareName='周度交易胜率') + '\n'
        weekSuccessText = '(1)交易胜率层面来看，' + weekSuccessText

        riskReturnDf = pd.read_excel(resultPath + "风险收益统计指标原始数据.xlsx", index_col=[0, 1])
        chengLiRiskReturnDf = riskReturnDf.loc['成立以来']
        chengLiAnnualDic = chengLiRiskReturnDf.loc['年化收益'].to_dict()
        chengLiAnnualDic.pop('数据截止日期')
        annualReturnText = self.getCompareIndex(chengLiAnnualDic, fundName, compareName='年化收益') + '\n'
        annualReturnText = '(2)收益方面，' + annualReturnText

        chengLiStdDic = chengLiRiskReturnDf.loc['年化波动'].to_dict()
        chengLiStdDic.pop('数据截止日期')
        riskText = self.getCompareIndex(chengLiStdDic, fundName, compareName='年化波动', rightFlag=False)
        riskText = '(3)风险方面，' + riskText

        chengLiDownStdDic = chengLiRiskReturnDf.loc['下行风险'].to_dict()
        chengLiDownStdDic.pop('数据截止日期')
        downStdText = self.getCompareIndex(chengLiDownStdDic, fundName, compareName='下行风险', rightFlag=False) + '\n'
        riskText = riskText + '\n  从下行风险角度来看，' + downStdText

        chengLiDownDic = chengLiRiskReturnDf.loc['最大回撤'].to_dict()
        chengLiDownDic.pop('数据截止日期')
        downText = self.getCompareIndex(chengLiDownDic, fundName, compareName='最大回撤', rightFlag=False) + '\n'
        riskText = riskText + '  最大回撤方面，' + downText

        chengLiSharpDic = chengLiRiskReturnDf.loc['夏普比率'].to_dict()
        chengLiSharpDic.pop('数据截止日期')
        sharpText = self.getCompareIndex(chengLiSharpDic, fundName, compareName='夏普比率', formatFlag=False) + '\n'
        sharpText = '(4)投资效率来看，' + sharpText

        riskReturnText = riskReturnText + weekSuccessText + annualReturnText + riskText + sharpText
        totalText = gaiKuoTxt + riskReturnText

        indusrtyAndStyleText = '\n行业归因结果：\n'
        indusrtyRegressionDf = pd.read_excel(resultPath + "行业回归结果.xlsx", )
        bestIndustry = indusrtyRegressionDf.iloc[0].to_dict()
        industryText = "该产品拟合效果最好的行业为%s(%s)，其回归后的拟合R方为%.2f%%" % (
            bestIndustry['指数名称'], bestIndustry['指数代码'], bestIndustry['拟合R方'] * 100) + '\n'

        tempDf = indusrtyRegressionDf[indusrtyRegressionDf['拟合R方'] >= 0.7]
        totalNum = 5 #最高的拟合行业数量
        if tempDf.empty:
            tempDf2 = indusrtyRegressionDf[indusrtyRegressionDf['拟合R方'] >= 0.1]
            if not tempDf2.empty:
                industryDetailTxt = "从行业回归结果来看，该产品无拟合效果较为优秀的行业指数（R方大于0.7），" \
                                    "这可能是用于产品运作过程中，持仓个股的行业变化较为频繁带来的，可结合进一步的持仓分析综合来看"
            else:
                industryDetailTxt = "从行业回归结果来看，该产品不存在具有一定相关性的行业指数（R方大于0.1），" \
                                    "这可能是用于产品运作过程中，持仓个股的行业变化极其频繁，行业分布极其分散，也可能是产品" \
                                    "有对冲市场beta风险的操作所带来的，可结合进一步的持仓分析，同时期的市场风险收益指标等综合来看"
        else:
            if tempDf.shape[0] > totalNum:
                codeList = tempDf.iloc[:totalNum]['指数代码'].tolist()
                codeNameList = tempDf.iloc[:totalNum]['指数名称'].tolist()
            else:
                codeList = tempDf['指数代码'].tolist()
                codeNameList = tempDf['指数名称'].tolist()
            strList = []
            for code, codeName in zip(codeList, codeNameList):
                strList.append(codeName + '(%s)' % code)
            industryDetailTxt = "从行业回归结果来看，该产品拟合效果较为优秀的行业指数（R方大于0.7）主要有%s，" \
                                "可对比该产品的投资类型给出判断。" % (','.join(strList))+'\n'
        indusrtyAndStyleText = indusrtyAndStyleText + industryText + industryDetailTxt

        indusrtyAndStyleText = indusrtyAndStyleText + '\n风格归因结果：\n'
        styleRegressionDf = pd.read_excel(resultPath + "风格回归结果.xlsx", )
        bestStyle = styleRegressionDf.iloc[0].to_dict()
        styleText = "该产品拟合效果最好的风格指数为%s(%s)，其回归后的拟合R方为%.2f%%" % (
            bestStyle['指数名称'], bestStyle['指数代码'], bestStyle['拟合R方'] * 100) + '\n'

        tempStyleDf = styleRegressionDf[styleRegressionDf['拟合R方'] >= 0.7]
        totalStyleNum = 3  # 最高的拟合风格数量
        if tempStyleDf.empty:
            tempStyleDf2 = styleRegressionDf[styleRegressionDf['拟合R方'] >= 0.1]
            if not tempStyleDf2.empty:
                styleDetailTxt = "从风格回归结果来看，该产品无拟合效果较为优秀的风格指数（R方大于0.7，" \
                                    "这可能是用于产品运作过程中，基金经理投资风格较为灵活，可结合进一步的持仓分析综合来看"
            else:
                styleDetailTxt ="从风格回归结果来看，该产品不存在具有一定相关性的风格指数（R方大于0.1，" \
                "这可能是用于产品运作过程中，基金经理投资风格极其灵活多变，或采用了衍生品对冲系统风险带来的，可结合进一步的持仓分析综合来看"
        else:
            if tempStyleDf.shape[0] > totalStyleNum:
                codeStyleList = tempStyleDf.iloc[:totalNum]['指数代码'].tolist()
                codeStyleNameList = tempStyleDf.iloc[:totalNum]['指数名称'].tolist()
            else:
                codeStyleList = tempStyleDf['指数代码'].tolist()
                codeStyleNameList = tempStyleDf['指数名称'].tolist()
            strStyleList = []
            for code, codeName in zip(codeStyleList, codeStyleNameList):
                strStyleList.append(codeName + '(%s)' % code)
            styleDetailTxt = "从风格回归结果来看，该产品拟合效果较为优秀的风格指数（R方大于0.7）主要有%s，" \
                                "可对比该产品的投资风格给出判断。" % (','.join(strStyleList)) + '\n'
        indusrtyAndStyleText = indusrtyAndStyleText + styleText + styleDetailTxt
        totalText = totalText + indusrtyAndStyleText
        f = open(resultPath + "综合评价结论.txt", "w+")
        f.write(totalText)
        f.close()

if __name__=="__main__":
    JudgeTextDemo = JudgeText()
    pass
