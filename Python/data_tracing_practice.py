import sys
import os
import pandas as pd


## Overview 
# Use memorized approach to save resources on further data quality checks
# Certain sections are concealed to safeguard confidential information.


# data_list/report_list: full list of required data/report 
# data_status/report_status: completed data/ report
# data remain/ report remain: remaining data need to check / report need to run ( total - completed )

class data_pool:
    def __init__(self,
                 save_path,
                 data_list,
                 report_list,
                 data_status=pd.DataFrame(columns=['source_table', 'status']),
                 report_status=pd.DataFrame(columns=['report', 'export_status']),
                 table_col_cnt=None,
                 ):
         
        self.data_list = data_list
        self.report_list = report_list
        self.data_status = data_status
        self.report_status = report_status
        self.data_remain = data_list['Table'].unique().tolist()
        self.report_remain = list(report_list.keys())
        self.save_path=save_path
        self.table_col_cnt=table_col_cnt if table_col_cnt is not None else {}
        self.update_table_col_cnt(self.data_list['Table'])
        
    def save_status(self):
        # Save the entire data_pool instance to the save_path
        with open(self.save_path, 'wb') as f:
            pickle.dump(self, f)
        #print(f"data_pool object saved to {self.save_path}")    

    def update_table_col_cnt(self,input_list):

        for table in input_list:
          self.table_col_query(table)
    
    
    def table_col_query(self,table):
        try:
          # Attempt to get column count with the primary query
          query = f'''SELECT COUNT(*) FROM {table}'''
          col_cnt = query_bq(query).iloc[0, 0]
          self.table_col_cnt[table] = col_cnt
    
        except Exception as e:  # Catching general exceptions for logging, consider narrowing it down
          try:
              # If the primary query fails, attempt the alternative query
              query = f'''SELECT COUNT(*) FROM {table} WHERE --filter condition for data quality -- '''
              col_cnt = query_bq(query).iloc[0, 0]
              self.table_col_cnt[table] = col_cnt

          except Exception as e_alt:
              # Log the error or handle it as necessary
              #print(f"Error with table {table} using both queries: {e}, {e_alt}")
              self.table_col_cnt[table] = None  # Set a default value or handle the error as appropriate         


    def update_data_status(self, input_status):
        if input_status is not None and not input_status.empty:
            combined_status = pd.concat([self.data_status, input_status]).reset_index(drop=True)
            
            combined_status = combined_status.sort_values(by='status', ascending=False)
            self.data_status = combined_status.drop_duplicates(subset=['source_table'], keep='first').reset_index(drop=True)
            self.data_remain = list(set(self.data_list['short_table']) - set(self.data_status[self.data_status['status']==1]['source_table']))
            self.save_status()
        else:
            pass
            #print("No new data status to update.")

    def update_report_status(self, input_status):
        if input_status is not None:
            input_status = pd.DataFrame({'report': [input_status], 'export_status': [1]})
        else:
            input_status = pd.DataFrame(columns=['report', 'export_status'])

        combined_status = pd.concat([self.report_status, input_status]).reset_index(drop=True)
        self.report_status = combined_status[combined_status['export_status'] == 1].drop_duplicates()
        self.report_remain = list(set(self.report_list.keys()) - set(self.report_status['report']))
        self.save_status()


    def force_data_status(self, table_list):
        if table_list:
            # Convert table_list to DataFrame for updating data_status
            table_list=[re.sub(data_pool.remove_prefix(),'',i) for i in table_list]
            table_df = pd.DataFrame({'source_table': table_list, 'status': 1})
            self.update_data_status(table_df)
            # Update data_remain to remove items that are now in the data_status
            self.data_remain = [item for item in self.data_remain if item not in table_list]
        else:
            pass
            #print("No tables provided to force status.")

    def generate_remain_data(self):
        if not self.data_status.empty:
            data_completed = self.data_status[self.data_status['status'] == 1]
            data_list_inst = self.data_list.copy()
            self.data_remain = data_list_inst[~data_list_inst['short_table'].isin(data_completed['source_table'])]['Table'].unique().tolist()
        return self.data_remain

    def generate_remain_report(self, completed_report=[]):
        if not self.report_status.empty:
            report_completed = self.report_status[self.report_status['export_status'] == 1]
            self.report_remain = [k for k, v in self.report_list.items() if k not in report_completed['report'].values]

        if completed_report:
            self.report_remain = [k for k in self.report_remain if k not in completed_report]

        return self.report_remain
    @staticmethod
    def remove_prefix():
        remove_strings = ['remove redundant prefix']
        remove_string_pattern = '|'.join(remove_strings)
        return remove_string_pattern
      
    @staticmethod
    def generate_data_status(table_list):

        # Dummy implementations for external checks, replace with actual logic
        check_snap = dailysnap_check.check_dailysnap(source_system=None,table_list=table_list)
        check_dupnew = dailysnap_check.check_dup_new(source_system=None,table_list=table_list)
        df_checkpoint = exp_data_tracing.checkpoint_v1(exp_data_tracing.pre_checkpoint_input_dict)


        #Temporary comment out this -not yet
        for df in [check_snap, check_dupnew, df_checkpoint]:
            df['source_table'] = df['source_table'].str.replace(data_pool.remove_prefix(), '', regex=True)

        df_data_status = pd.merge(check_snap, check_dupnew, on='source_table', how='left')
        df_data_status = pd.merge(df_data_status, df_checkpoint, on='source_table', how='left')
        df_data_status.fillna(value=1, inplace=True)

        status_columns = [col for col in df_data_status.columns if col.startswith('status_')]
        df_data_status[status_columns] = df_data_status[status_columns].astype(int)
        df_data_status['status'] = df_data_status[status_columns].min(axis=1)

        return df_data_status[['source_table', 'status']]

    @staticmethod
    def generate_report_status(query_dict, df_data_status):
        if query_dict:
          df_tracing = exp_data_tracing.summary_tracing(query_dict)
          df_tracing['secondary_path']=df_tracing[df_tracing['source_table'].str.contains('business_data')]['source_table']\
                      .apply(lambda x: exp_data_tracing.generate_source_uris(*x.split('.')).split('/',3)[-1].replace('/*',''))
          df_summary_path=exp_data_tracing.summary_path(query_dict)
          df_status=exp_data_tracing.summary_export(query_dict)
  
  
          # Merging tracing with summary path
          df_tracing = pd.merge(df_tracing, df_summary_path, left_on='secondary_path', right_on='path', how='left')
          df_tracing = df_tracing.iloc[:, [0, 1, -1]]
          df_tracing.columns = ['report', 'source_table', 'secondary_report']
  
          exc_not_same_source = (df_tracing['report'].str.upper().str.startswith('core_data_source') & df_tracing['secondary_report'].str.upper().str.startswith('branch_data_source')) | \
                                (df_tracing['report'].str.upper().str.startswith('branch_data_source') & df_tracing['secondary_report'].str.upper().str.startswith('core_data_source'))
          df_tracing = df_tracing[~exc_not_same_source]
  
          remove_strings = ['remove redundant prefix']
          remove_string_pattern = '|'.join(remove_strings)
          df_tracing['source_table'] = df_tracing['source_table'].str.replace(remove_string_pattern, '', regex=True)
  
          df_tracing_computation = pd.merge(df_tracing, df_data_status, on='source_table', how='left').fillna(1)
  
          # Pivot table to find the minimum status for each report
          df_tracing_lite = pd.pivot_table(df_tracing_computation, index=['report', 'secondary_report'], values='status', aggfunc='min').reset_index()
          df_tracing_lite.columns = ['report', 'secondary_report', 'status']
          df_tracing_lite=data_pool.mapping_dependency(df_tracing_lite,'report','secondary_report')
          # Manually extracted reports considered as pass
          df_tracing_lite['status'] = np.where(df_tracing_lite['report'].isin(df_status[df_status['export_status'] == 1]['report'].unique()), 1, df_tracing_lite['status'])
  
          df_tracing_lite['secondary_report'] = np.where(df_tracing_lite['secondary_report'].isna(), None, df_tracing_lite['secondary_report'])
          # Dummy recursive join, replace with actual logic
          df_tracing_lite['secondary_status'] = df_tracing_lite['status']
          df_tracing_lite['report_status'] = df_tracing_lite[['status', 'secondary_status']].min(axis=1)
  
          df_tracing_lite = pd.pivot_table(df_tracing_lite, index='report', values='report_status', aggfunc='min').reset_index()
          df_tracing_lite.columns = ['report', 'report_status']
          df_eligible_report = df_tracing_lite[['report', 'report_status']]
          df_eligible_report.columns = ['report', 'status']
          df_eligible_report = pd.merge(df_tracing_computation[['report']], df_eligible_report, on='report', how='left')
  
          df_eligible_report = pd.merge(df_eligible_report, df_status, on='report', how='left').fillna(0)
  
          eligible_data = df_eligible_report.loc[(df_eligible_report['status'] == 1) & (df_eligible_report['export_status'] == 0), 'report'].unique()
          pending_report = df_eligible_report.loc[(df_eligible_report['status'] == 0)&(df_eligible_report['export_status']==0), 'report'].unique()
          completed_report = df_eligible_report.loc[(df_eligible_report['export_status'] == 1), 'report'].unique()
          #print(eligible_data,pending_report,completed_report)
          
          for item in eligible_data:
              log_GCS('job_name', f'recommend {item}')
          for item in pending_report:
              log_GCS('job_name', f'pending {item}')
          for item in completed_report:
              log_GCS('job_name', f'completed {item}')
              
          return eligible_data, completed_report
        else:
          return None, None

    #@staticmethod  
    def process_queries(self, input_list=None):
        if input_list is not None and len(input_list)>0:
          query_dict = self.report_list.copy()

          for key, value in query_dict.items():
              if key in input_list:
                  #print(key)
                  #print(value)
                  row_cnt = query_bq(value).iloc[0, 0]
                  
                  if row_cnt > 0:
                      log_GCS('job_name', f'{key}')
                      #print(key)
                      self.update_report_status(key)
                  else:
                      log_GCS('job_name', f'Stop processing at {key}')
                      break
        else:
          pass

    @staticmethod
    def error_handler(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log_GCS('job_name', f"error {func.__name__}: {e}")
        return wrapper

    @staticmethod
    def process_secondary():
        run_nbfi_preprocess = data_pool.error_handler(nbfi_preprocess)
        run_lnbr_external = data_pool.error_handler(lnbr_external)
        run_solvency_report_extract = data_pool.error_handler(solvency_report_extract)

        run_nbfi_preprocess()
        run_lnbr_external(numdays=0)
        run_solvency_report_extract(numdays=0)
        
        # Data Integrity check for regulatory report
        data_integrity_check.temporal_consistency_check(data_integrity_check.df_mapping_temoporal_consistency)
        data_integrity_check.null_col_check(data_integrity_check.df_null_col_mapping)
        data_integrity_check.non_sensical_check()
        if datetime.now().strftime('%A') == 'Monday':  # weekly decree 53 data mart on Monday
            run_dxx_migration = data_pool.error_handler(dxx_migration)
            run_dxx_migration()
      
    #@staticmethod
    def logging(self):
      for item in self.data_remain:
        log_GCS('job_name',f'remain data {item}')
      
      for item in self.report_remain:
        log_GCS('job_name',f'remain report {item}')

    @staticmethod        
    def mapping_dependency(df,col1,col2):
      def update_status(row, df):
          # Skip rows where col2 equals 1
          if row[col2] == 1:
              return row['status']
          
          if pd.notna(row[col2]):
              # Find the row in col1 that matches col2
              matched_row = df[df[col1] == row[col2]]
              if not matched_row.empty:
                  return matched_row['status'].values[0]
          return row['status']
    
      # Initialize a flag to track changes
      status_changed = True
    
      # Loop until no more status changes
      while status_changed:
          # Copy the current status to detect changes after the update
          previous_status = df['status'].copy()
          
          # Apply the update to all rows
          df['status'] = df.apply(update_status, axis=1, df=df)
          
          # Check if the status changed
          status_changed = not df['status'].equals(previous_status)
      
      return df

# Implement

# Schedule freq
schedule_freq = '37 8-20 * * *' if dailysnap_check.is_workingday(datetime.now())==0 else '37 8-20 * * *'


def job_name_flow_v3(logging=True):
  df_dailysnap=pd.read_csv('data/dailysnap.csv')
  df_dailysnap=df_dailysnap[df_dailysnap['Check']==1]
  remove_strings = ['remove redundant prefix']
  remove_string_pattern = '|'.join(remove_strings)

  df_dailysnap['short_table']= df_dailysnap['Table'].str.replace(data_pool.remove_prefix(), '', regex=True)
  
  
  report_list_dict=dailysnap_check.data_dict
  report_list_dict.update(dailysnap_check.onep_dict)
  # Load pickle / Create new if not exist 
  pickle_path = f"data_pkl/data_pool_{datetime.now().strftime('%Y%m%d')}.pkl"
  

  if os.path.exists(pickle_path):
      # Load the pickle file
      with open(pickle_path, 'rb') as file:
          data_pool_inst = pickle.load(file)
      #print("Pickle file loaded.")
  else:
      # Create a new pickle file
      data_pool_inst = data_pool(pickle_path,df_dailysnap, report_list_dict)
      with open(pickle_path, 'wb') as file:
          pickle.dump(data_pool_inst, file)
          
  if logging:
    data_pool_inst.logging()

  # Data checking
  if data_pool_inst.data_remain:
    data_status=data_pool.generate_data_status(data_pool_inst.data_remain)
  
  # Update completed data / remain data 
    data_pool_inst.update_data_status(data_status)
    data_pool_inst.generate_remain_data()
    data_pool_inst.update_table_col_cnt(data_pool_inst.data_remain)
  else:
    #pass
    data_status=data_pool_inst.data_status
  # Report checking
  cur_query_dict={k:v for k,v in report_list_dict.items() if k in data_pool_inst.report_remain}
  
  
  eligible_report,completed_report=data_pool.generate_report_status(cur_query_dict,data_status)
  
  remaining_reports = set(data_pool_inst.report_remain)
  #print(completed_report)
  #print(remaining_reports)
  
  if completed_report is not None and len(completed_report) > 0:
      for item in completed_report:
          if item in remaining_reports:
              data_pool_inst.update_report_status(item)
            

  # Run report
  data_pool_inst.process_queries(input_list=eligible_report)
  #data_pool_inst.generate_remain_report(completed_report=completed_report)
  
  # Post checkpoint 
  exp_data_tracing.checkpoint_v1(exp_data_tracing.post_checkpoint_input_dict)

    
  with open(pickle_path, 'wb') as file:
      pickle.dump(data_pool_inst, file)    
    
  # Check remain report - If no remain report run secondary reports 
  if len(data_pool_inst.report_remain)==0:
    #  if datetime.now().strftime('%A') not in ['Saturday','Sunday']:
    data_pool.process_secondary()

    exp_data_tracing.update_job_schedule('job_name','daily runtime here')
  
  else:
    exp_data_tracing.update_job_schedule('job_name',schedule_freq)

