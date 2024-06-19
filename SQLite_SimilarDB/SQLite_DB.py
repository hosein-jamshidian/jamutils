import sqlite3 as db
import pandas as pd 
import sqlalchemy
import os

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
    
    


if __name__ == '__main__':
    backup_db= SqliteDB(path= 'D:/projects/SQLite_SimilarDB/HJ.db')
    design= '(id INTEGER PRIMARY KEY, sale_id INTEGER, user_id INTEGER, sale_datetime NUMERIC, ex_datetime NUMERIC)'
    backup_db.create_table("sales", column_design= design)
    data= pd.DataFrame({"name": ['a','b','c','d'], "age":[20,21,22,23]})
    backup_db.update_table(data= data, table_name=' sales')
    query= '''select * from sales'''
    results= backup_db.get_data( query= query, query_params= None)
    