#import sys
#import os
#sys.path.append(os.path.abspath('/Workspace/Repos/kumar.senniappan@nbcuni.com/data-eng'))
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import numpy as np
import scipy.stats as stat
from sqlalchemy.dialects import registry
import pandas as pd
import CONSTAINTS as c
# from NBCUDomoConnector import NBCUDomoConnector
import multiprocessing as mp
import functools
import logging
import struct
import sys
import time
import pytz
import pandas as pd



def timer(start, end):
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Execution time: {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))

    
def now(status):
    est = pytz.timezone('US/Eastern')
    current_time = datetime.now(est).strftime("%-I:%M:%S %p %Z %x")
    print(status, current_time)

def snowflake_engine():
    registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
    
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse_name}'.format(
            user='wrose',
            password='Temporary2020!',
            account='nbcd.us-east-1',
            database='WALDO_PROD',
            schema='mparticle',
            warehouse_name='WH_DE_DEV'
        )
    )
    return engine


def run_sql(engine, query_name, query):
    print('\nGetting ' + query_name + ' ...')
    now('Query started at: ')
    start = time.time()

    # Execute sql query
    result_iter = pd.read_sql_query(query, engine, chunksize=1000000)
   
    now('Query finished at: ')
    timer(start, time.time())
    now('Reading result: ')
    start = time.time()
    dfl = []
    for chunk in result_iter:
        dfl.append(chunk)
     
    now('Read finished at: ')
    timer(start, time.time())
    
    return pd.concat(dfl)

