"""
__author__ = Biswajit Saha

this script creates sqllite schema and batch-loads  all csv od matrix
"""


import sqlite3
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer,Float, String, MetaData,Index
import pandas as pd
from pathlib import Path
import time
import logging

def create_logger():
    # logger = multiprocessing.get_logger()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
 
    fh = logging.FileHandler("db.log")
    fh.setLevel(logging.INFO)
 
    fmt = '%(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    fh.setFormatter(formatter)
 
    logger.addHandler(fh)
    #adding console handler
    consolehandler = logging.StreamHandler()
    consolehandler.setLevel(logging.DEBUG)
    consolehandler.setFormatter(formatter)
    logger.addHandler(consolehandler)
    return logger

logger = create_logger()

def measure(message):
    
    def decorator(function):
        def wrapper(*args, **kwargs):
            ts =time.time()
            result = function(*args, **kwargs)
            te = time.time()
            minutes_taken = "minutes taken {}".format((te-ts)/60.0)

            logger.info(f"{message}  {minutes_taken}")
            
            return result
        return wrapper
    return decorator


class Db:

    def __init__(self,name):
        self.dbname =name
        self.engine = create_engine(f"sqlite:///{self.dbname}.sqlite3")
    
    def create_table(self,if_exists_drop=False):
        metadata = MetaData()


        MATRIX = Table('matrix', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('meshblock_id', String(50)),
                      Column('centre_name', String(100)),
                      Column('stop_id', String(50)),
                      Column('time_of_day', String(50)),
                      Column('total_minutes', Float),
                      Index("IDX_CENTRENAME_TIMEOFDAY_MINUTES", "centre_name", "time_of_day",'total_minutes'  )

                      )
       
        
        if MATRIX.exists(self.engine):
            if if_exists_drop:
                MATRIX.drop(self.engine)
                MATRIX.create(self.engine)
                print('table re-created')
            else:
                print('table already exists')
        else:
             MATRIX.create(self.engine)
             print('new table created')
        self.table = MATRIX

    def reindex(self):
        #self.engine.execute(sql)
        pass
    
    @measure('total:')
    def load_csv(self,csv):
        df = pd.read_csv(csv)
        df.to_sql('matrix',self.engine,if_exists='append',index =False)
        print(f'{csv} loaded')

    
    def __del__(self):
        self.engine.dispose()

@measure('Grand Total:')      
def load_all_csvs():
    mydb = Db('CentreAccesByMb')
    mydb.create_table(True)
    csvdir = Path().cwd().parent.joinpath('output').joinpath('matrix')
    for idx, f in enumerate(csvdir.glob('*.csv')):
        mydb.load_csv(f)
        print(f'{idx+1} completed ...')

    del mydb
    print('done!')


if __name__ =='__main__':
    load_all_csvs()


    
 
