# -*- coding: utf-8 -*-
"""
Created on Sun Aug 29 18:55:46 2021

@author: JWeiz
"""

import os
import pandas as pd

root_dir = r'C:\New_Model'
directory_852 = r'C:\New_Model\852'
proj_dir = r'C:\New_Model\Projections'
item_ref_filename = 'Item Reference.csv'
dc_item_settings_filename = 'Item Settings.xlsx'
top_level_adj_filename = 'Top Level Adjustments.xlsx'


#----------------create DC data table---------------------

big_852_df = pd.DataFrame()

for filename in os.listdir(directory_852):
    little_df = pd.read_csv(directory_852+'/'+filename)
    big_852_df = big_852_df.append(little_df)

dc_data = pd.melt(big_852_df, 
                  id_vars = ['Location','Item_Cod'],
                  value_vars = ['CODE-QA','CODE-QW', 'CODE-QP', 'CODE-QD', 
                                'CODE-QO', 'CODE-QC', 'CODE-QX','CODE-OQ'],
                  )
#=========================================================

#----------------------create proj table------------------
proj_df = pd.DataFrame()

for filename in os.listdir(proj_dir):
    df = pd.read_excel(proj_dir+'/'+filename, engine = 'openpyxl',
                              header = 0
                              )
    melted_df = pd.melt(df,id_vars = ['DC','LAZY CODE'])
    proj_df = proj_df.append(melted_df)

#=========================================================

#-----------------create item reference table-------------
item_ref = pd.read_csv(root_dir+'/'+item_ref_filename)
#=========================================================

#-----------------create top-level adjustment table-------------
top_level_adj_raw = pd.read_excel(root_dir+'/'+top_level_adj_filename)
top_level_adj = pd.melt(top_level_adj_raw, id_vars = 'DC')
#=========================================================

#-----------------create DC-item settings table----------------
dc_item_settings_raw = pd.read_excel(root_dir+'/'+dc_item_settings_filename)
dc_item_settings = pd.melt(dc_item_settings_raw,id_vars = ['DC','Item'])
#=========================================================

#-----------------------create PO table-------------------


#=========================================================

def plan_row(dc,item):
    

