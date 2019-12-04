from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import json
import os
import logging
import pandas as pd
import numpy as np

def load_properties():
        with open( 'C:/Users/Mohd.Humza/Desktop/Notebook/gcp/Srinisalesapp2/properties/properties.json' ) as fin:
                SETTING = json.load( fin )
        return SETTING

def env():
    SETTING = load_properties()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SETTING['KEY_FILE']
    credentials = service_account.Credentials.from_service_account_file(
                                        'C:/Users/Mohd.Humza/Desktop/Notebook/gcp/Srinisalesapp2/credentials/orderanalyticsapp1-b6263ba87422.json')
    bqclient = bigquery.Client(credentials=credentials,project=SETTING['PROJECT'],)
    return credentials,bqclient

def transformation(dataframe_ss,dataframe_loc, dataframe_sid):
    newData = pd.DataFrame()
    #ss = ss.fillna(0)
    filter_values = ['S','X','R119']
    dataframe_ss = dataframe_ss[dataframe_ss['SJCd'].str.contains('|'.join(filter_values), na=False)]
    dataframe_ss.reset_index(inplace = True)
    newData['TimeDayId'] = dataframe_ss['FinancialDayDt'].str.replace('-','')
    
    
    locid = []
    for i,x in dataframe_ss[['RevenueLocNo','BusinessUnitCd']].iterrows():
        for i_loc,x_loc in dataframe_loc[['LocNo','Level6Cd']].iterrows():
            if (x.RevenueLocNo==x_loc.LocNo) and (x.BusinessUnitCd == x_loc.Level6Cd):
                locid.append(dataframe_loc.at[i_loc,'LocId'])
            else:
                locid.append(0)
    newData['locId'] = locid
    
    dataframe_ss['Amount'] = dataframe_ss['Amount'].astype('float')
    net_amount = []
    i = 0
    for r in dataframe_ss['SJCd']:
        if r.startswith('S'):
            net_amount.append(dataframe_ss.at[i,'Amount']*-1)
            i+=1
    
        elif r.startswith('X'):
            net_amount.append(dataframe_ss.at[i,'Amount'])
            i+=1
    
        else: 
            net_amount.append(0)
    newData['NetAmount'] = net_amount
    
    discount = []
    for i,r in enumerate(dataframe_ss['SJCd']):
        if r.startswith('X'):
            discount.append(dataframe_ss.at[i,'Amount'])
        else:
            discount.append(0)
    newData['DiscountAmount'] = discount        
    
    ClickSold = []
    for i,r in enumerate (dataframe_ss['SJCd']):
        if r.startswith('R119'):
            ClickSold.append(dataframe_ss.at[i,'ClickSold'])
        else:
            ClickSold.append(0)
    newData['TransCount'] = ClickSold
    
    SourceSystemId = []
    for r in dataframe_ss['SourceSystem']:
        for idx,c in enumerate(dataframe_sid['SourceSystemCd']):
            if(r==c):
                SourceSystemId.append(dataframe_sid.at[idx,'SourceSystemId'])
    newData['SourceSystemId'] = SourceSystemId
        
    
    return newData


def read_from_table():
    credentials,bqclient = env()
    table_ss = bigquery.TableReference.from_string("orderanalyticsapp1.salessummarydata.salessummary_stage")
    rows_ss = bqclient.list_rows(table_ss,selected_fields=[
        bigquery.SchemaField("SlNo", "STRING"),
        bigquery.SchemaField("RevenueLocNo", "STRING"),
        bigquery.SchemaField("LocNo", "STRING"),
        bigquery.SchemaField("TrxSetDt", "STRING"),
        bigquery.SchemaField("FinancialDayDt", "STRING"),
        bigquery.SchemaField("SourceSystem", "STRING"),
        bigquery.SchemaField("SJCd", "STRING"),
        bigquery.SchemaField("Amount", "STRING"),
        bigquery.SchemaField("ClickSold", "STRING"),
        bigquery.SchemaField("BusinessUnitCd", "STRING"),
        bigquery.SchemaField("QuantitySold", "STRING"),],)
    dataframe_ss = rows_ss.to_dataframe()
    dataframe_ss = dataframe_ss[1:]
    dataframe_ss.reset_index(inplace = True)
    table_loc = bigquery.TableReference.from_string("orderanalyticsapp1.salessummarydata.dwd_location")
    rows_loc = bqclient.list_rows(table_loc,selected_fields=[
        bigquery.SchemaField("LocId", "INTEGER"),
        bigquery.SchemaField("LocNo", "STRING"),
        bigquery.SchemaField("CurrentFl", "STRING"),
        bigquery.SchemaField("LocDesc", "STRING"),
        bigquery.SchemaField("BranchYN", "STRING"),
        bigquery.SchemaField("StateCd", "STRING"),
        bigquery.SchemaField("CountryCd", "STRING"),
        bigquery.SchemaField("OpenDt", "STRING"),
        bigquery.SchemaField("CloseDt", "STRING"),
        bigquery.SchemaField("Level1Cd", "STRING"),
        bigquery.SchemaField("Level1Desc", "STRING"),
        bigquery.SchemaField("Level2Cd", "STRING"),
        bigquery.SchemaField("Level2Desc", "STRING"),
        bigquery.SchemaField("Level3Cd", "STRING"),
        bigquery.SchemaField("Level3Desc", "STRING"),
        bigquery.SchemaField("Level4Cd", "STRING"),
        bigquery.SchemaField("Level4Desc", "STRING"),
        bigquery.SchemaField("Level5Cd", "STRING"),
        bigquery.SchemaField("Level5Desc", "STRING"),
        bigquery.SchemaField("Level6Cd", "STRING"),
        bigquery.SchemaField("Level6Desc", "STRING"),
        bigquery.SchemaField("CountryDesc", "STRING"),],)
    dataframe_loc = rows_loc.to_dataframe()
    table_sid = bigquery.TableReference.from_string("orderanalyticsapp1.salessummarydata.dwd_source_system")
    rows_sid = bqclient.list_rows(table_sid,selected_fields=[
        bigquery.SchemaField("SourceSystemId", "INTEGER"),
        bigquery.SchemaField("SourceSystemCd", "STRING"),
        bigquery.SchemaField("SourceSystemDesc", "STRING"),],)
    dataframe_sid = rows_sid.to_dataframe()
    dataframe = transformation(dataframe_ss,dataframe_loc, dataframe_sid)
    
    pandas_gbq.context.credentials = credentials
    pandas_gbq.to_gbq(dataframe,destination_table='salessummarydata.dwf_daily_branch_sales',project_id='orderanalyticsapp1',credentials=credentials,
                     if_exists='replace')
    logging.info( 'Pushing data from BigQuery' )          




    



