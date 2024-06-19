import sqlalchemy


db_creds= {'0': {
    'name': '',
    'username': '',
    'password': '',
    'server': '',
    'database': '',
    'port': '3306',
    'connector': 'mysql+pymysql' ,
    'type': 'mysql'
    },
    }

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
'''