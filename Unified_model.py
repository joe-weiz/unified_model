# -*- coding: utf-8 -*-
"""
Created on Sun Aug 29 18:55:46 2021

@author: JWeiz
"""

import os
import pandas as pd
import numpy as np
import sqlite3 as sql
import math

root_dir = r'C:\New_Model'
directory_852 = r'C:\New_Model\852'
proj_dir = r'C:\New_Model\Projections'
item_ref_filename = 'Item Reference.csv'
dc_ref_filename = 'DC Reference.csv'
dc_item_settings_filename = 'Item Settings.xlsx'
top_level_adj_filename = 'Top Level Adjustments.xlsx'
po_table_filename = 'PO Table.xlsx'
date_ref_filename = 'Date Reference.xlsx'

'''
CODE - QA	BOH
CODE - QW	PULLS
CODE - QP	INBOUND
CODE - QO	SCRATCHES
CODE-QC	COMMITTED
CODE-QX	
CODE-OQ	DEMAND
'''

con = sql.connect(r'C:\New_Model\Unified_Model.db')
cur = con.cursor()

#----------------create DC data table---------------------

big_852_df = pd.DataFrame()

for filename in os.listdir(directory_852):
    little_df = pd.read_csv(directory_852+'/'+filename)
    big_852_df = big_852_df.append(little_df)

big_852_df.to_sql('DC_DATA', con, if_exists = 'replace')
#=========================================================

#----------------------create proj table------------------
proj_df = pd.DataFrame()

for filename in os.listdir(proj_dir):
    df = pd.read_excel(proj_dir+'/'+filename, engine = 'openpyxl',
                              header = 0
                              )
    melted_df = pd.melt(df,id_vars = ['DC','LAZYCODE'],var_name = 'Date')
    proj_df = proj_df.append(melted_df)

proj_df.to_sql('PROJ', con, if_exists = 'replace')
#=========================================================

#-----------------create item reference table-------------
item_ref = pd.read_csv(root_dir+'/'+item_ref_filename)
item_ref.to_sql('item_ref', con, if_exists = 'replace')
#=========================================================

#-----------------create dc reference table-------------
dc_ref = pd.read_csv(root_dir+'/'+dc_ref_filename)
dc_ref.to_sql('dc_ref', con, if_exists = 'replace')
#=========================================================

#-----------------create top-level adjustment table-------------
top_level_adj_raw = pd.read_excel(root_dir+'/'+top_level_adj_filename)
top_level_adj = pd.melt(top_level_adj_raw, id_vars = 'DC')
top_level_adj.to_sql('top_level_adj', con, if_exists = 'replace')

#=========================================================

#---------------create DC-item settings table-------------
dc_item_settings_raw = pd.read_excel(root_dir+'/'+dc_item_settings_filename)
dc_item_settings = pd.melt(dc_item_settings_raw,id_vars = ['DC','Item'])
dc_item_settings.to_sql('DC_ITEM_SETTINGS', con, if_exists = 'replace')

#=========================================================

#---------------------create PO table---------------------
po_table_raw = pd.read_excel(root_dir+'/'+po_table_filename)
po_table = pd.melt(po_table_raw, id_vars = ['DC','Item'], var_name = 'Date')
po_table.to_sql('po_table', con, if_exists = 'replace')
#=========================================================

#-------------create Date Reference table-----------------
date_ref = pd.read_excel(root_dir+'/'+date_ref_filename)
date_ref.to_sql('date_ref', con, if_exists = 'replace')
#=========================================================

con.commit()


#----------------------Functions--------------------------
# I think rather than making one function that can pull data from any table,
# I will create separate functions for each table. This will make the
# api and filtering logic simpler.

# The function below should return the 852 boh data for a 
# DC/Item combination. It will use the pandas read_sql function.

def boh_852(dc,item):

    query = f'''
    select [CODE-QA] from dc_data 
    inner join dc_ref on dc_data.Location = dc_ref.Location
    inner join item_ref on dc_data.Item_Cod = item_ref.GTIN
    where
    dc_ref.DC = '{dc}'
    and 
    item_ref.LAZYCODE = '{item}'
    '''
    
    boh = pd.read_sql(query, con)
    
    #The below error handling doesn't work
    # try:
    #     boh.count() == 1
    # except:
    #     print("BOH query returned more than one result. Check for duplicate entries.")
    
    result = int(boh.iloc[0][0])

    return result


def demand_852(dc,item):

    query = f'''
    select [CODE-OQ] from dc_data 
    inner join dc_ref on dc_data.Location = dc_ref.Location
    inner join item_ref on dc_data.Item_Cod = item_ref.GTIN
    where
    dc_ref.DC = '{dc}'
    and 
    item_ref.LAZYCODE = '{item}'
    '''
    
    boh = pd.read_sql(query, con)
        
    result = int(boh.iloc[0][0])

    return result


def proj(dc,item,date):

    
    query = f'''
    select value from PROJ 
    where
    PROJ.DC = '{dc}'
    and 
    PROJ.LAZYCODE = '{item}'
    and
    PROJ.Date = '{pd.to_datetime(date)}'
    '''

    boh = pd.read_sql(query, con)
        
    result = int(boh.iloc[0][0])

    return result


def plan_row(dc,item,daylist):
    #Note: Instead of trying to handle the 852 BOH initialization inside this
    #function, I should just make a SQL query earlier that copies the
    #852 BOH values over to the BOH table
    
    boh_df = pd.DataFrame()
    ship_df = pd.DataFrame()
    print('Line 178')
    #The first day's boh comes from the 852
    boh = boh_852(dc,item)
    day = daylist[0]
    boh_insert_query = f'''
        Insert into BOH(DC,Item,Date,Value) 
        VALUES ('{dc}','{item}','{pd.to_datetime(day)}',{boh})
        '''
    print('Line 186')
    cur.execute(boh_insert_query)
    con.commit()
    
    print(item)
    #This fstring will be used in the loop below
    boh_query = f'''select value from BOH 
        where
        BOH.DC = '{dc}'
        and BOH.Item = '{item}'
        and BOH.Date = '{day}'
        '''

    for day in (daylist[0], daylist[-1]):
                
        cur.execute(boh_query)
        boh = cur.fetchone()[0]
        demand = proj(dc,item,pd.to_datetime(day))
        
        ship = demand-boh
        ship_df[day] = ship
        
        boh_df[day + np.timedelta64(1,'D')] = boh + ship - demand
        

#=========================================================

#----------------------Create tables-------------------------
#clear BOH table


query = '''
CREATE TABLE IF NOT EXISTS BOH(
	DC data_type TEXT,
   	Item data_type TEXT,
	Date data_type TIMESTAMP,
    Value data_type Integer
)
'''
con.execute(query)
con.commit()


query = 'delete from BOH;'
con.execute(query)
con.commit()
#============================================================



#-----------------------Generate ship qty's-------------------




daylist = proj_df['Date'].unique()


for day in daylist:
    p = proj('Atlanta','D6AMER',day)
    print(p)

    for x in range(0,3):
        #Using integer location below because index has repeat values
        dc = proj_df.iloc[x][0]
        item = proj_df.iloc[x][1]
        plan_row(dc, item, daylist)

# def plan_ship_qty(day,dc,item):
#     #ship_q = 



#=========================================================
    
dc = 'Atlanta'
item = 'D6AMER'
    
boh_df = pd.DataFrame()
ship_df = pd.DataFrame()

#daylist = proj_df['Date'].unique()

query = f'''
insert into 






select value from PROJ 
where
PROJ.DC = '{dc}'
and 
PROJ.LAZYCODE = '{item}'
and
PROJ.Date = '{pd.to_datetime(date)}'
'''

cursor.execute(query)

boh_df[(dc,item,daylist[0])] = boh_852(dc,item)

for day in (daylist[0], daylist[-1]):
    
    bboh = boh_df[(dc, item, pd.to_datetime(day))]
    demand = proj(dc,item,pd.to_datetime(day))
    
    ship = max(demand-bboh,0)
    ship_df[day] = ship
    
    boh_df[day + np.timedelta64(1,'D')] = bboh + ship - demand
    
    
    
    