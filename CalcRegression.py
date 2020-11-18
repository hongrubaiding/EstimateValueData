# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com
'''
    回归类结果分析与保存
    选股择时能力：TM,HM ,CL
    行业回归
    风格回归
'''
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from datetime import datetime, timedelta
import statsmodels.api as sm

matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['font.family'] = 'sans-serif'
matplotlib.rcParams['axes.unicode_minus'] = False


class CalcRegression:
    def __init__(self):
        pass

    def regression(self, x1, x2, y):
        '''
        最小二乘回归
        :param x1:
        :param x2:
        :param y:
        :return:
        '''
        x1, x2 = x1.reshape(len(x1), 1), x2.reshape(len(x2), 1)
        c = np.ones((len(x1), 1))
        X = np.hstack((c, x1, x2))
        res = (sm.OLS(y, X)).fit()
        return res

    def getStyleRegression(self,fundIndustryDf, resultPath,fundName,industryDic,DCIndexDf=pd.DataFrame()):
        '''
        风格归因
        :param fundIndustryDf:
        :param resultPath:
        :param fundName:
        :param industryDic:
        :return:
        '''
        industryCodeList = list(industryDic.keys())
        targetLabel = industryCodeList + [fundName]
        targetDf = fundIndustryDf[targetLabel]
        tempReturn = (targetDf - targetDf.shift(1)) / targetDf.shift(1)
        if not DCIndexDf.empty:          #量化类产品
            bench_return_df = DCIndexDf/DCIndexDf.shift(1)-1
            tempReturn[fundName] = pd.concat([bench_return_df,tempReturn[fundName]],axis=1,sort=True).sum(axis=1)

        def reduceRf(tempSe):
            resultSe = tempSe - fundIndustryDf['无风险利率']
            return resultSe

        tempExReturn = tempReturn.apply(reduceRf)
        tempExReturn.dropna(inplace=True)

        list_r2, list_beta, list_tr, list_const = [], [], [], []
        Y = tempExReturn[fundName].values
        for code in industryCodeList:
            x = tempExReturn[code].values
            x = x.reshape(len(x), 1)
            c = np.ones((len(x), 1))
            X = np.hstack((c, x))
            res = (sm.OLS(Y, X)).fit()
            list_r2.append(res.rsquared)
            list_beta.append(res.params[1])
            list_const.append(res.params[0])
            list_tr.append((fundIndustryDf[code][-1] / fundIndustryDf[code][0] - 1) - fundIndustryDf['无风险利率'].mean())
        res_indus = pd.DataFrame([])
        res_indus['指数代码'] = industryCodeList
        res_indus['指数名称'] = [industryDic[code] for code in industryCodeList]
        res_indus['拟合R方'] = list_r2
        res_indus['beta'] = list_beta
        res_indus['alpha'] = list_const
        res_indus['期间总收益'] = list_tr
        res_indus['开始时间'] = tempExReturn.index.tolist()[0]
        res_indus['终止时间'] = tempExReturn.index.tolist()[-1]
        res_indus = res_indus.sort_values('拟合R方', ascending=False)
        res_indus.to_excel(resultPath + '风格回归结果.xlsx', index=False)

        maxR2Code = res_indus['指数代码'].tolist()[0]
        x = tempExReturn[maxR2Code].values
        maxR2Alpha = res_indus['alpha'].tolist()[0]
        maxR2Beta = res_indus['beta'].tolist()[0]

        plt.style.use('ggplot')
        plt.figure(figsize=(16, 9))
        plt.scatter(x, Y, s=30, color='blue', label='样本实例')
        plt.plot(x, maxR2Alpha + maxR2Beta * x, linewidth=3, color='red', label='回归线')
        plt.ylabel('产品超额收益')
        plt.xlabel('风格超额收益')
        plt.title('拟合效果最好的风格指数：' + industryDic[maxR2Code], fontsize=13,
                  bbox={'facecolor': '0.8', 'pad': 5})
        plt.grid(True)
        plt.legend(loc='upper left')  # 添加图例
        plt.savefig(resultPath + '拟合风格指数效果图.png')
        # plt.show()

        plt.style.use('ggplot')
        fig = plt.figure(figsize=(16, 9))
        ax = fig.add_subplot(111)
        indeustryAccDf = (1 + tempReturn[[fundName, maxR2Code]]).cumprod()
        indeustryAccDf['产品风格收益比'] = indeustryAccDf[fundName] / indeustryAccDf[maxR2Code]
        indeustryAccDf.plot(ax=ax)
        ax.set_ylabel('累计收益率')
        ax.set_xlabel('时间')
        ax.set_title('拟合效果最好的风格指数：' + industryDic[maxR2Code], fontsize=13,
                     bbox={'facecolor': '0.8', 'pad': 5})
        ax.grid(True)
        ax.legend(loc='down right')  # 添加图例
        plt.savefig(resultPath + '拟合风格指数累计走势对比图.png')
        plt.show()

    def getIndustryRegression(self,fundIndustryDf, resultPath,fundName,industryDic,bench_return=pd.DataFrame()):
        '''
        行业归因
        :param fundIndustryDf:
        :param resultPath:
        :param fundName:
        :param industryDic:
        :return:
        '''
        industryCodeList = list(industryDic.keys())
        targetLabel = industryCodeList+[fundName]
        targetDf = fundIndustryDf[targetLabel]

        tempReturn = (targetDf-targetDf.shift(1))/targetDf.shift(1)
        if not bench_return.empty:          #量化类产品
            bench_return_df = bench_return/bench_return.shift(1)-1
            tempReturn[fundName] = pd.concat([bench_return_df,tempReturn[fundName]],axis=1,sort=True).sum(axis=1)
        def reduceRf(tempSe):
            resultSe = tempSe - fundIndustryDf['无风险利率']
            return resultSe
        tempExReturn = tempReturn.apply(reduceRf)
        tempExReturn.dropna(inplace=True)

        list_r2, list_beta, list_tr,list_const = [], [], [],[]
        Y = tempExReturn[fundName].values
        for code in industryCodeList:
            x = tempExReturn[code].values
            x = x.reshape(len(x), 1)
            c = np.ones((len(x), 1))
            X = np.hstack((c, x))
            res = (sm.OLS(Y, X)).fit()
            list_r2.append(res.rsquared)
            list_beta.append(res.params[1])
            list_const.append(res.params[0])
            list_tr.append((fundIndustryDf[code][-1] / fundIndustryDf[code][0]-1) - fundIndustryDf['无风险利率'].mean())
        res_indus = pd.DataFrame([])
        res_indus['指数代码'] = industryCodeList
        res_indus['指数名称'] = [industryDic[code] for code in industryCodeList]
        res_indus['拟合R方'] = list_r2
        res_indus['beta'] = list_beta
        res_indus['alpha'] = list_const
        res_indus['期间总收益'] = list_tr
        res_indus['开始时间'] = tempExReturn.index.tolist()[0]
        res_indus['终止时间'] = tempExReturn.index.tolist()[-1]
        res_indus = res_indus.sort_values('拟合R方', ascending=False)
        res_indus.to_excel(resultPath+'行业回归结果.xlsx',index=False)

        maxR2Code = res_indus['指数代码'].tolist()[0]
        x = tempExReturn[maxR2Code].values
        maxR2Alpha = res_indus['alpha'].tolist()[0]
        maxR2Beta = res_indus['beta'].tolist()[0]

        plt.style.use('ggplot')
        plt.figure(figsize=(16, 9))
        plt.scatter(x, Y, s=30, color='blue', label='样本实例')
        plt.plot(x, maxR2Alpha + maxR2Beta * x, linewidth=3, color='red', label='回归线')
        plt.ylabel('产品超额收益')
        plt.xlabel('行业超额收益')
        plt.title('拟合效果最好的行业指数：'+industryDic[maxR2Code], fontsize=13,
                  bbox={'facecolor': '0.8', 'pad': 5})
        plt.grid(True)
        plt.legend(loc='upper left')  # 添加图例
        plt.savefig(resultPath+'拟合行业指数效果图.png')
        # plt.show()

        plt.style.use('ggplot')
        fig=plt.figure(figsize=(16, 9))
        ax = fig.add_subplot(111)
        indeustryAccDf = (1+tempReturn[[fundName,maxR2Code]]).cumprod()
        indeustryAccDf['产品行业收益比'] = indeustryAccDf[fundName]/indeustryAccDf[maxR2Code]
        indeustryAccDf.plot(ax=ax)
        ax.set_ylabel('累计收益率')
        ax.set_xlabel('时间')
        ax.set_title('拟合效果最好的行业指数：'+industryDic[maxR2Code], fontsize=13,
                  bbox={'facecolor': '0.8', 'pad': 5})
        ax.grid(True)
        ax.legend(loc='down right')  # 添加图例
        plt.savefig(resultPath + '拟合行业指数累计走势对比图.png')

    def getSelectStockAndTime(self, fundPlotDf, resultPath, fundName, netPeriod, benchMark,DCIndexDf=pd.DataFrame()):
        '''
        计算选股择时能力
        :param ReturnData:
        :return:
        '''

        if netPeriod == 'W':
            calcPeriod = 52
        else:
            calcPeriod = 250

        if not DCIndexDf.empty:
            target_df = pd.concat([fundPlotDf[[fundName,benchMark]],DCIndexDf],axis=1,sort=True)
            tempReturn = (target_df - target_df.shift(1)) / target_df.shift(1)
            tempReturn.fillna(0, inplace=True)
            tempReturn[fundName] = tempReturn[[tempReturn.columns[0],tempReturn.columns[-1]]].sum(axis=1)       #量化对冲产品
        else:
            targetDf = fundPlotDf[[fundName, benchMark]]
            tempReturn = (targetDf - targetDf.shift(1)) / targetDf.shift(1)
            tempReturn.fillna(0, inplace=True)

        fundReduceRf = tempReturn[fundName] - fundPlotDf['无风险利率']
        bencReduceRf = tempReturn[benchMark] - fundPlotDf['无风险利率']

        f = open(resultPath + "TM,HM,CL模型回归结果.txt", "w+")
        Y = fundReduceRf.values
        tmX1 = bencReduceRf.values
        tmX2 = np.power(tmX1, 2)
        TMResult = self.regression(tmX1, tmX2, Y)

        dicRegression = {}
        dicRegression['TM回归结果'] = {}
        dicRegression['TM回归结果']['R方'] = TMResult.rsquared
        dicRegression['TM回归结果']['择股指标(年化alpha)'] = str(round(TMResult.params[0] * calcPeriod * 100, 2)) + '%'
        dicRegression['TM回归结果']['择时指标(beta)'] = round(TMResult.params[2], 2)
        f.write(str(TMResult.summary(title='TM模型回归结果')))
        f.write('\n\n\n')

        d = []  # H-M模型
        for i in range(len(tempReturn[benchMark])):
            if tempReturn[benchMark][i] > fundPlotDf['无风险利率'][i]:
                d.append(1)
            else:
                d.append(0)
        hmX1 = bencReduceRf.values
        hmX2 = d * hmX1 ** 2
        HMResult = self.regression(hmX1, hmX2, Y)
        dicRegression['HM回归结果'] = {}
        dicRegression['HM回归结果']['R方'] = HMResult.rsquared
        dicRegression['HM回归结果']['择股指标(年化alpha)'] = str(round(HMResult.params[0] * calcPeriod * 100, 2)) + '%'
        dicRegression['HM回归结果']['择时指标(beta)'] = round(HMResult.params[2], 2)
        f.write(str(HMResult.summary(title='HM模型回归结果')))
        f.write('\n\n\n')

        x1, x2 = [], []  # C-L模型
        for i in range(len(tempReturn[benchMark])):
            if tempReturn[benchMark][i] > fundPlotDf['无风险利率'][i]:
                x1.append(tempReturn[benchMark][i] - fundPlotDf['无风险利率'][i])
                x2.append(0)
            else:
                x1.append(0)
                x2.append(tempReturn[benchMark][i] - fundPlotDf['无风险利率'][i])
        clX1, clX2 = np.array(x1), np.array(x2)
        CLResult = self.regression(clX1, clX2, Y)
        dicRegression['CL回归结果'] = {}
        dicRegression['CL回归结果']['R方'] = CLResult.rsquared
        dicRegression['CL回归结果']['择股指标(年化alpha)'] = str(round(CLResult.params[0] * calcPeriod * 100, 2)) + '%'
        dicRegression['CL回归结果']['择时指标(beta)'] = round(CLResult.params[2] - CLResult.params[1], 2)

        regressionDf = pd.DataFrame(dicRegression)
        regressionDf.to_excel(resultPath + '选股择时能力回归结果.xlsx')
        f.write(str(CLResult.summary(title='CL模型回归结果')))
        f.close()
        return


if __name__ == '__main__':
    CalcRegressionDemo = CalcRegression()
