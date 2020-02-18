
"""
__author__ = Biswajit Saha

this multiprocessing script creates service  area  matrix for every centre
"""




import pandas as pd
from sqlalchemy import create_engine
import logging
import time
import multiprocessing
import sys
from pathlib import Path



def create_logger():
    # logger = multiprocessing.get_logger()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
 
    fh = logging.FileHandler("service_area.log")
    fh.setLevel(logging.ERROR)
 
    fmt = '%(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    fh.setFormatter(formatter)
 
    logger.addHandler(fh)
    #adding console handler
    consolehandler = logging.StreamHandler()
    consolehandler.setLevel(logging.INFO)
    consolehandler.setFormatter(formatter)
    logger.addHandler(consolehandler)
    return logger

logger = create_logger()

def minutes_taken(t1,t2):
    return "minutes taken {}".format((t2-t1)/60.0)




def create_sql(start_time,end_time,centre_name, travel_time):
    
    sql ="""
        SELECT  matrix.meshblock_id, matrix.centre_name, count(*) n, max(matrix.total_minutes) max_time, min(matrix.total_minutes) min_time from matrix 
        where
        matrix.centre_name ='{}'
        AND DATETIME(matrix.time_of_day) between DATETIME('{}') and DATETIME('{}')
        AND matrix.total_minutes <={}
        GROUP BY matrix.meshblock_id, matrix.centre_name
        ORDER BY matrix.meshblock_id

        """.format(centre_name,start_time,end_time, travel_time)
    return sql

def calculate(centre_name):

    script_dir = Path().cwd()
    sa_dir = script_dir.parent.joinpath('output').joinpath('servicearea')

    if not sa_dir.exists():
        sa_dir.mkdir()

    engine =  create_engine('sqlite:///CentreAccesByMb.sqlite3')
    ts = time.time()
    logger.info(f'processing ..{centre_name}')

    query_variables= [
        ['2019-11-04 07:00:00','2019-11-04 09:00:00',0.0],
        ['2019-11-04 07:00:00','2019-11-04 09:00:00',3.0],
        ['2019-11-04 07:00:00','2019-11-04 08:00:00',0.0],
        ['2019-11-04 07:00:00','2019-11-04 08:00:00',3.0],
        ['2019-11-04 08:00:00','2019-11-04 09:00:00',0.0],
        ['2019-11-04 08:00:00','2019-11-04 09:00:00',3.0]
    ]
    dfs=[]
    for start_time, end_time, waiting_time in query_variables:
        #import pdb; pdb.set_trace()

        travel_time =30.0 -waiting_time
        sql = create_sql(start_time,end_time,centre_name, travel_time)
        df = pd.read_sql(sql,engine)
        fmt = '%Y-%m-%d %H:%M:%S'
        df['start_time'] = pd.to_datetime(start_time,format=fmt)
        df['end_time'] = pd.to_datetime(end_time,format=fmt)
        df['waiting_minutes']= waiting_time
        df['label'] = df.apply(lambda x: f"{x['start_time'].strftime('%H:%M')}-{x['end_time'].strftime('%H:%M')}",axis=1)
        df.to_csv(sa_dir.joinpath(centre_name+'.csv'),index=False)
        dfs.append(df)
        print(centre_name,start_time, end_time, waiting_time)
    te = time.time()
    msg = f"{centre_name}  {minutes_taken(ts,te)}"
    logger.info(msg)
    engine.dispose()
    final_df= pd.concat(dfs,axis=0)
    final_df.to_csv(sa_dir.joinpath(centre_name+'.csv'), index =False)
    return final_df

def main():
    ts =time.time()

    
   

    # chunks = create_chunks()
    # print(chunks)

 


    df_centres= pd.read_csv('centres.csv')
    chunks =df_centres['centrename'].tolist()

 

   
   

    #change number of worker here
 
    try:
        pool = multiprocessing.Pool(processes=4)
        results =[]
        for chunk in chunks:
            results.append(pool.apply_async(calculate,args=(chunk,)))
        
        dfs =[result.get() for result in results]
    except Exception as e:
        logger.error("multiprocessing failed due to {}".format(str(e)))
        import traceback
        tb = sys.exc_info()[2]
        print ("An error occured on line %i" % tb.tb_lineno)
       
       
   
        
    
    finally:
        pool.close()
        pool.join()
    te = time.time()
    message = "total {} centres completed and {}".format(len(chunks), minutes_taken(ts,te))
    logger.info(message)
    return pd.concat(dfs,axis=0)

if __name__ == "__main__":
    df= main()
    df.to_csv('servicearea.csv', index = False)

    print('done')
    
