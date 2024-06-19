import sqlite3 as db
import pandas as pd 
import sqlalchemy
import os
import datetime as dt
from SQLite_SimilarDB.query import sales_query, engine
from SQLite_SimilarDB.SQLite_DB import SqliteDB

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

if __name__ == '__main__':
    SALES_QP= {}
    
    sales= CreateSimilarDB('D:/HJ.db',
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


