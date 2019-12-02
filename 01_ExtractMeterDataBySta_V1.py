# -*- coding: utf-8 -*-
"""
1. 程序目的：依据站点号重新整理V3.0版本数据存储格式-按站点存储

2. 北京师范大学  2019年09月25日  星期三

3. 数据
   3.1 输入数据
   (1) r'G:\ChinaMeteroDataProcessing2018\01_OriginalData\02_ClimDataV30'路径下的
      8种类型气象数据【.txt格式】
   (2) 站点信息文件
      'G:\ChinaMeteroDataProcessing2018\01_OriginalData\01_BasicData\ChinaSta839.csv'
      
   3.2 输出数据
   (1) 'G:\ChinaMeteroDataProcessing2018\03_DataProcessingResult\01_ClimDataV30_Sta\'
       路径下8个文件夹里的数据
     
4. 备注
   (1) 程序的算法思路是
      (a) 首先将某一气象类型的所有站点存放在一个DataFrame中
      (b) 按站点提取数据
   (2) 待改进之处
      (a) 将某一气象类型同时存放在一个数据框中，在数据较多时，容易出现的问题是占据大量的
          内存，程序运行报错，需要尝试采取新的处理思路
      (b) 程序的容错性需要进一步检查，比如某一个文件中某一站点不存在，数据的分隔符存在多种怎么处理？
"""

# 0. 相关包的导入
import os
import shutil
import numpy as np
import pandas as pd


# 1. 基本函数定义
  # 1.1 读取某一类型气象要素所有数据 
'''
  (1) 输入参数
      1)InPath-路径 2)styr-提取开始年 3)edyr-提取结束年 4)typestr-气象要素类型【命名】
  (2) 输出结果
      ClimateDataAll：提取的所有气象数据      
'''
def ClimateDataAll_Get(inpath,styr,edyr,typestr):
    ClimateDataAll = pd.DataFrame() #  创建一个完全空的数据框，作为数据追加的开始
    for year in np.arange(styr,edyr+1):
        for month in np.arange(1,12+1):        
            ClimateFile = inpath + typestr + str(year*100+month)+'.TXT'
            ClimateDataTemp = pd.read_csv(ClimateFile,header=None,delim_whitespace=True)
            ClimateDataAll =  ClimateDataAll.append(ClimateDataTemp)
            print(year,month)
    return ClimateDataAll

  # 1.2 提取特定站点的数据并写出
'''
  (1) 输入参数
      1) DataAll - 某一气候类型所有数据
      2) Sta_ID - 某一站点号
      3) OutFile - 输出文件的完整路径
'''
def StaData_Get(DataAll,Sta_ID,OutFile):
    ClimSta = DataAll.loc[DataAll[0] == Sta_ID] #特定站点数据提取
    ClimSta.to_csv(OutFile,index=False,header=None) # 忽略index和header
    
#***********************需要修改的部分********************************#    
# 2. 路径处理和基本变量定义
RootDir = r'G:\ChinaMeteroDataProcessing2018'
StaFile = RootDir + '\\01_OriginalData\\01_BasicData\\ChinaSta839.csv'

StaID_All = (pd.read_csv(StaFile))['Sta']

InPath = RootDir+'\\01_OriginalData\\02_ClimDataV30\\'
TypeStr = input("请输入要提取的气象要素：")
#TypeStr = 'SURF_CLI_CHN_MUL_DAY-TEM-12001-'
ClimType = TypeStr.split('-')[1]
OutPath = RootDir+'\\03_DataProcessingResult\\01_ClimDataV30_Sta'\
          + 'China_' + ClimType + '_'

StYr = 1978; EdYr = 2018

OutPath = RootDir+'\\03_DataProcessingResult\\01_ClimDataV30_Sta\\'\
          + 'China_' + ClimType + '_' + str(StYr) + '-' +str(EdYr)
if os.path.exists(OutPath):
    shutil.rmtree(OutPath)
    os.makedirs(OutPath)
else:
    os.makedirs(OutPath)

#***********************需要修改的部分********************************#  

# 3.相关函数调用 
  # 3.1 调用ClimateDataAll_Get函数，读取某一类型气象要素所有数据
ClimateDataAll = ClimateDataAll_Get(InPath,StYr,EdYr,TypeStr)

  # 3.2 调用StaData_Get()函数，按站点获取数据并写出
for Sta_ID in StaID_All:
    OutFile = OutPath + '\\' + str(Sta_ID) + '_' + TypeStr + str(StYr)\
              + str(EdYr) + '.csv'
    StaData_Get(ClimateDataAll,Sta_ID,OutFile)
    print(Sta_ID)

print('**********************************************************************')

print('Finished')
