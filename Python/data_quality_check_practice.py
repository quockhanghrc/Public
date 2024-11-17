import sys
import os


# Practice to measure data quality: data availability, data integrity
# Certain sections are concealed to safeguard confidential information.

df_dailysnap=pd.read_csv('data/Material/dailysnap.csv')
df_dailysnap=df_dailysnap[df_dailysnap['Check']==1]
remove_strings = ['remove redundant prefix']
remove_string_pattern = '|'.join(remove_strings)
df_dailysnap['short_table']= df_dailysnap['Table'].str.replace(remove_string_pattern, '', regex=True)

def get_query_for_item(item):
    current_date_func = "current_date('Asia/Ho_Chi_Minh')"
    current_datetime_func="current_datetime('Asia/Ho_Chi_Minh')"
    #specific conditions base on what type of tables 
    return query

def check_dailysnap(source_system, table_list=None, get_log=1, get_detail=1):
    transactiondate = datetime.now() - timedelta(days=1)
    if source_system:
      source_system = source_system.upper()
    column_list = []
    error_list = []
    empty_list = []

    if table_list:
      check_list=df_dailysnap['Table'][ (df_dailysnap['Type'] == 'TABLE') & (df_dailysnap['Table'].isin(table_list))]
    else:
      check_list=df_dailysnap['Table'][(df_dailysnap['Source'] == source_system) & (df_dailysnap['Type'] == 'TABLE')]
    
    for item in check_list:
        query = get_query_for_item(item)
        try:
            try:
                df_len = client.query(query).result().total_rows 
            except:
                query_2 = query.replace('master_created_date', 'date(master_created_date_time)')
                df_len = client.query(query_2).result().total_rows

            if ((item in ['transaction_data']) and (not is_workingday(transactiondate))) or \
                    ((item == 'disbursement_data') and (datetime.now().strftime('%A') != 'Monday')):
                pass
            else:
                column_list.append([item, df_len])

            if df_len == 0:
                empty_list.append(item)
                log_GCS('job_name', 'no snapshot ' + item)
        except:
            error_list.append(item)
            log_GCS('job_name', 'error snap ' + item)
    
  
    if get_detail == 0:
        func_output = 0 if min(column_list, key=lambda x: x[1])[1] == 0 else 1
    else:
        func_output = pd.DataFrame(column_list)
        func_output.columns = ['source_table', 'status']
        func_output['status'] = np.where(func_output['status'] > 0, 1, 0)

    return func_output


def compare_daily(table,default_query):
  result_df=query_bq(default_query)
  if len(result_df)>0:
    result_supp_onep = 1
    
    #Duplication
    if (result_df['id']!=result_df['unique_id']).loc[0]:
      log_GCS('job_name',table+' duplication')  
    
    # Missing key records 
    if result_df['missing_id'][0]>0:
      log_GCS('job_name',table+' missing key records')  
      
    # Increasing records
    if ((result_df['id']>=result_df['prev_id']).loc[0] and result_df['prev_id'][0]>=0) \
    or (table=='PMS' and (result_df['id']>=result_df['prev_id']*0.999).loc[0] and result_df['prev_id'][0]>=0) \
    or (table=='branch_source' and (result_df['id']>=result_df['prev_id']*0.999).loc[0] and result_df['prev_id'][0]>=0)  \
    or (table=='branch_source' and result_supp_onep==1)  : 
      return 1

    else:
      log_GCS('job_name',table+' no increase')
      return 0



def check_dup_new(source_system, table_list=None, return_df=True):
    if source_system:
      source_system = source_system.upper()
    result_data = []
    
    if table_list:
      check_list=df_dailysnap[(df_dailysnap['Key'].notnull()) &  (df_dailysnap['Table'].isin(table_list))]
    else:
      check_list=df_dailysnap[(df_dailysnap['Key'].notnull()) & (df_dailysnap['Source'] == source_system)]
    
    for index, row in check_list.iterrows():
        key = row["Key"]
        table = row["Table"]
        #print(table)
        cur_key = copy.copy(key)
        prev_key = key.replace('cur', 'prev')

        default_query = f'''DECLARE reportingdate DATE DEFAULT current_date('Asia/Ho_Chi_Minh');
                            DECLARE prevdate DATE DEFAULT date_sub(current_date('Asia/Ho_Chi_Minh'), interval 1 day);
                            
                            SELECT IFNULL(cur.master_created_date, reportingdate) as master_created_date,
                                COUNT(cur.id) as id,
                                COUNT(DISTINCT cur.id) as unique_id,
                                COUNT(prev.id) as prev_id ,
                                COUNT( case when cur.id is null and prev.id is not null then 1 end) missing_id
                            FROM 
                                (SELECT master_created_date, {cur_key} as id FROM {table} cur WHERE master_created_date = reportingdate) cur
                                FULL OUTER JOIN 
                                (SELECT master_created_date, {prev_key} as id FROM {table} prev WHERE master_created_date = prevdate) prev 
                                ON cur.id = prev.id
                            GROUP BY 1'''

        if table == 'DailySnap_CustomerInfo':
            onep_query = f'''DECLARE reportingdate DATE DEFAULT current_date('Asia/Ho_Chi_Minh');
                            DECLARE prevdate DATE DEFAULT date_sub(current_date('Asia/Ho_Chi_Minh'), interval 1 day);

                            SELECT IFNULL(cur.master_created_date, reportingdate) as master_created_date,
                                COUNT({cur_key}) as id,
                                COUNT(DISTINCT {cur_key}) as unique_id,
                                COUNT({prev_key}) as prev_id  
                                COUNT( case when {cur_key} is null and {prev_key} is not null then 1 end) missing_id

                            FROM 
                                ((SELECT master_created_date, cust_no FROM {table} WHERE master_created_date = reportingdate AND source_type = '1P') cur
                                FULL OUTER JOIN 
                                (SELECT cust_no FROM {table} WHERE master_created_date = prevdate AND source_type = '1P') prev 
                                ON {cur_key} = {prev_key})
                            GROUP BY 1'''

            icore_query = f'''DECLARE reportingdate DATE DEFAULT current_date('Asia/Ho_Chi_Minh');
                            DECLARE prevdate DATE DEFAULT date_sub(current_date('Asia/Ho_Chi_Minh'), interval 1 day);

                            SELECT IFNULL(cur.master_created_date, reportingdate) as master_created_date,
                                COUNT({cur_key}) as id,
                                COUNT(DISTINCT {cur_key}) as unique_id,
                                COUNT({prev_key}) as prev_id
                                COUNT( case when {cur_key} is null and {prev_key} is not null then 1 end) missing_id

                            FROM 
                                ((SELECT master_created_date, cust_no FROM {table} WHERE master_created_date = reportingdate AND source_type = 'icore' AND cust_no NOT IN ('100000589987','100000598050')) cur
                                FULL OUTER JOIN 
                                (SELECT cust_no FROM {table} WHERE master_created_date = prevdate AND source_type = 'icore' AND cust_no NOT IN ('100000589987','100000598050')) prev 
                                ON {cur_key} = {prev_key})
                            GROUP BY 1'''
            try:
              result_data.append(('cust 1P', compare_daily('cust 1P', onep_query)))
            except:
              continue
              #print(onep_query)
            try:
              result_data.append(('cust icore', compare_daily('cust icore', icore_query)))
            except:
              continue
              #print(icore_query)
        else:
            try:
              result_data.append((table, compare_daily(table, default_query)))
            except:
              log_GCS('job_name', 'error compare ' + table)
              #pass
            
    # recheck this. want if no table check then make a blank dataframe with all 1 
    if len(result_data)==0 and table_list is not None:
      for item in table_list:
        result_data.append((item,1))
    
    result_df = pd.DataFrame(result_data, columns=['source_table', 'status'])
    #return result_df
    
    result = 0 if min(result_df['status']) == 0 else 1
    
    if return_df:
        return result_df
    else:
        return result

  
def is_workingday(datetime_input):
    df_holidays = pd.to_datetime(pd.read_csv('data/holidays.csv')['holiday']).dt.date
    is_weekend = datetime_input.strftime('%A') in ['Saturday', 'Sunday']
    is_holiday = datetime_input.date() in df_holidays.values
    return int(not is_weekend and not is_holiday)
