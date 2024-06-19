import sqlite3 as db
import pandas as pd 
import sqlalchemy
import os
import datetime as dt



class SqliteDB:
    def __init__(self, path):
        self.path= path
        self.conn= ''
        self.cur= ''
        self.connector= ''
        self.query= ''
        self.data=pd.DataFrame()
    
    def _primary_conection(self):
        '''connction with dbsqlite DB.'''
        
        self.conn= db.connect(self.path)
        self.cur= self.conn.cursor()
        
        
    def create_table(self, table_name: str, column_design: str):
        '''create a table with desire variabl types.'''
        
        self._primary_conection()
        creating= f'''CREATE TABLE IF NOT EXISTS {table_name} {column_design}'''
        self.cur.execute(creating)
        self.conn.commit()
    
    def add_new_col(self, table_name: str, col_name: str, col_type:str):
        '''add a new variable to an existing table'''
        
        self._primary_conection()
        adding= f'''ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type} '''
        self.cur.execute(adding)
        self.conn.commit()
        
    def remove_col(self, table_name: str, col_name: str):
        '''remove a variable from an existing table.'''
        
        self._primary_conection()
        removing = f'''ALTER TABLE {table_name} DROP COLUMN {col_name}'''
        self.cur.execute(removing)
        self.conn.commit()
        
        
    def _evacuate_table(self, table_name):
        '''remove desire table variables value except columns name. '''
        self._primary_conection()
        self.cur.execute(f'''DELETE FROM {table_name}''')
        self.conn.commit()
        
        
    def update_table(self, data, table_name):
        '''update table. 
        implemented srategy is : evacuate previous data and replacing current values.'''
        
        self._evacuate_table(table_name)
        data= data.copy()
        data= data.drop(['id'], axis= 1, errors= 'ignore')
        data= data.reset_index()
        tune_data= [tuple(i) for i in data.to_dict(orient='split')['data'] if 'id' not in data.columns]
        alignments= ", ".join([str('?') for i in range(len(data.columns))])
        
        self.cur.executemany(f'''INSERT OR IGNORE INTO {table_name} VALUES
                        ({alignments})''', tune_data)
                        
        self.conn.commit()
        
        
    def _connect(self):
        '''connect to sqlalchemy'''
        
        if os.path.exists(self.path):
            self.connector= sqlalchemy.create_engine('''sqlite:///{cred}'''.format(cred= self.path))
        else:
            raise ImportError("DB Doesn't Exist")


    def _get_query(self, query, query_params= None):
        '''tune query'''
        
        try: 
            self.query= query.format(**query_params) if query_params is not None else query
        except: 
            raise SyntaxError('Something Wrong With query_params')
        
        
    def get_data(self, query, query_params= None):
        '''get and read data from SQLiteDbB'''
        
        self._connect()
        self._get_query(query, query_params)
        try:
            self.data=  pd.read_sql(self.query, self.connector)
        except: 
            ValueError('Something Wrong With query')
        else:
            return self.data
    
    def __repr__(self):
        return 'SqliteDB'

#-----------------------------------------------------------------------------

class CreateSimilarDB:
    def __init__(self,
                  path,
                  table_name,
                  datetime_col_name,
                  all_datetime_col_names,
                  query,
                  query_parameters,
                  engine,
                  sort_by,
                  duplicate_by
                  ):
        
        self.path= path
        self.datetime_col_name= datetime_col_name
        self.all_datetime_col_names= all_datetime_col_names
        self.query= query
        self.table_name= table_name
        self.query_parameters= query_parameters
        self.engine= engine
        self.sort_by= sort_by
        self.duplicate_by= duplicate_by
        self.sqliteDB= SqliteDB(path)
        self.results= pd.DataFrame()
    
    
    def get_previous_data(self):
        previous_data_query= f'''SELECT * FROM {self.table_name}'''
        previous_data= self.sqliteDB.get_data(previous_data_query)
        return previous_data
    
    
    def get_last_update(self, data= ''):
        if isinstance(data, pd.DataFrame):
            last_created_at= pd.to_datetime(data[self.datetime_col_name]).max()
            
        elif isinstance(data, str):
            last_created_at= pd.to_datetime("20230416")
            
        else:
            raise ValueError('Invalid input value.')
        return last_created_at
            
        
    def update_query_parameters(self, data= ''):
        last_created_at= self.get_last_update(data)
        self.query_parameters.update({"start_date":'"' + str(last_created_at) + '"'})
        return self.query_parameters
    
    def mysql_get_data(self):
        current_data= pd.read_sql(self.query.format(**self.query_parameters), self.engine)
        return current_data
     

    def data_processing(self, data):
        data= data.sort_values(self.sort_by)
        data= data.drop_duplicates(self.duplicate_by, ignore_index= True)
        for col in self.all_datetime_col_names:  
            data[col]= pd.to_datetime(data[col])
        return data
    
    def create_operation(self):
        if os.path.exists(self.path):
            previous_data= self.get_previous_data()
            self.query_parameters= self.update_query_parameters(previous_data)
            current_data= self.mysql_get_data()
            
            data= pd.concat([previous_data, current_data], ignore_index= True)
            self.results= self.data_processing(data)
        else:
            self.query_parameters= self.update_query_parameters(data= '')
            data= self.mysql_get_data()
            self.results= self.data_processing(data)
            
        return self.results
    
    
    def export(self):
        for col in self.all_datetime_col_names: 
            self.results[col]= self.results[col].apply(lambda row: row.isoformat())
            
        try:
            self.sqliteDB.update_table(self.results, self.table_name)
        except: 
            column_design= input("Please give me design of you're SQLite DB:\n")
            self.sqliteDB.create_table(self.table_name, column_design)
            self.sqliteDB.update_table(self.results, self.table_name)
        return 'Done'

    
    def run(self):
        # breakpoint()
        self.results= self.create_operation()
        return self.export()


#-------------------------------------------------------------------------------
from configs.configs import (db_creds,
                             test_user_ids,
                             gs_creds, 
                             root_path, 
                             gs_logs)
                             
db_username = db_creds["0"]['username']
db_password = db_creds["0"]['password']
db_database = db_creds["0"]['database']
db_server = db_creds["0"]['server']
db_port = db_creds["0"]['port']
db_connector= db_creds["0"]['connector']

engine = sqlalchemy.create_engine("{}://{}:{}@{}:{}/{}".format(db_connector,
                                                               db_username,
                                                               db_password,
                                                               db_server,
                                                               db_port,
                                                               db_database))


sales_query= '''
SELECT
subscriptions.id as sale_id,
subscriptions.user_id,
subscriptions.created_at as sale_datetime,
subscriptions.expired_at as ex_datetime
from subscriptions 
INNER JOIN gateway_transactions ON subscriptions.id = gateway_transactions.gateway_transactionable_id
INNER JOIN subscription_packages ON subscriptions.subscription_package_id = subscription_packages.id
WHERE
subscriptions.deleted_at is NULL
AND gateway_transactions.deleted_at is NULL
AND gateway_transactions.transaction_status =2 
AND gateway_transactions.user_id not in ({test_ids})
'''


test_users_path= os.path.join(root_path,
                              'configs',
                              'test_users.csv')

test_users= pd.read_csv(test_users_path)
all_test_users= list(set(test_users['user_id'].tolist() + test_user_ids))
all_test_users= ",".join([str(i) for i in all_test_users])



if __name__ == '__main__':
    SALES_QP= {'test_ids':all_test_users}
    sales= CreateSimilarDB('D:/changaal_tasks/changaal',
                           'sales',
                           'sale_datetime',
                           ['sale_datetime','ex_datetime'],
                           sales_query,
                           SALES_QP,
                           engine,
                           ['sale_id','sale_datetime'],
                           ['sale_id']
                           )
    response= sales.run()

# '(id INTEGER PRIMARY KEY, sale_id INTEGER, user_id INTEGER, sale_datetime NUMERIC, ex_datetime NUMERIC)'


    results= sales.sqliteDB.get_data('''select * from sales''')


