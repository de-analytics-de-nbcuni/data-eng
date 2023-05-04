from util import *
import sql_queries as sql
import CONSTAINTS as c
from datetime import datetime, timedelta
import warnings
pd.set_option('display.max_columns', None)

warnings.filterwarnings("ignore")

# engine = snowflake_engine()

def rolling_7_days(engine, column_names, service_account):
    ############################################################################
    # Import Data
    df_raw = sql.run_rolling_visits_per_post_sql(engine, column_names, service_account)
    
    ############################################################################
    # Run Analysis
    
    
    # Union to have a value for referral platform being 'All'
    df_all = df_raw.copy()
    df_all['referral_platform'] = 'All'

    df = pd.concat([df_all, df_raw])

    # Change date to a data datatype
    df['publisheddate']= pd.to_datetime(df['publisheddate'])
    df['visit_date']= pd.to_datetime(df['visit_date'])

    # Calc the number of days between Visit Date and Publish Date (ensure not negitive...bad data)
    df['date_diff'] = (df['visit_date'] - df['publisheddate']).dt.days
    df = df.loc[df['date_diff'] >= 0]

    # If Published date is before the Insider page launched, then set Published date to first launch date
    nbc_first_date = datetime(2022, 2, 22)
    usa_first_date = datetime(2022, 3, 24)

    # First 7 days (>=6)
    # First 14 days (>=13)
    # First 21 days (>=20)
    # First 28 days (>=27)

    bucket_cutoffs = [7,14,21,28]
    for i in bucket_cutoffs:
        bucket_column_name = str(i) + '_bucket'
        df.loc[df['date_diff'] < i - 1, bucket_column_name] = 'yes' 
        df.loc[df['date_diff'] >= i - 1, bucket_column_name] = 'no'

    bucket_cutoffs = [str(x) for x in bucket_cutoffs]

    # Get Last Sunday Date
    today = datetime.today()
    idx = (today.weekday() + 1) % 7 # MON = 0, SUN = 6 -> SUN = 0 .. SAT = 6
    sunday = today - timedelta(7+idx)
    sunday = sunday.replace(hour=0, minute=0, second=0, microsecond = 0)

    # create an Empty DataFrame object
    df_final = pd.DataFrame()

    for bucket_cutoffs_temp in bucket_cutoffs:

        df_bucket_temp = df.loc[df[bucket_cutoffs_temp + '_bucket'] == 'yes']

        # Current date minus bucket cutoff day
        max_date_cutoff = datetime.today() - timedelta(days=int(bucket_cutoffs_temp))

        #Remove Articles that haven't been out for at least the bucket size
        df_bucket_temp = df_bucket_temp.loc[df_bucket_temp['publisheddate'] <= max_date_cutoff]

        df_group = df_bucket_temp.groupby(['brand', 'referral_platform' 
                                           , 'publisheddate', 'published_week']).agg(visit_count = ('visit_num', 'sum'))
        df_group['page_count'] = df_bucket_temp.groupby(['brand', 'referral_platform', 
                                                         'publisheddate', 'published_week']).contentid.nunique()

        df_group.reset_index(inplace=True)

        # Only look at full weeks
        df_group['publisheddate'] = df_group['publisheddate']#.dt.date
        df_group = df_group.loc[df_group['publisheddate'] < sunday - timedelta(7*(int(bucket_cutoffs_temp)/7-1))]

        df_group['bucket'] = bucket_cutoffs_temp

        df_final = df_final.append(df_group)

    df_final.reset_index(inplace=True, drop=True) 
    
    return df_final