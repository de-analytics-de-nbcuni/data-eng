#import sys
#import os
# In the command below, update with the repo that you want to pull into the cluster
#sys.path.append(os.path.abspath('/Workspace/Repos/kumar.senniappan@nbcuni.com/data-eng'))
#from util import *
import util
import sql_queries as sql
import CONSTAINTS as c
import warnings
import pandas as pd
# import datetime
from datetime import date
from pylatex import Document, PageStyle, Head, MiniPage, Foot, LargeText, \
    MediumText, SmallText, LineBreak, simple_page_number, Section, \
    Subsection, TextBlock, HugeText, VerticalSpace, HorizontalSpace, \
    Tabular, Math, TikZ, Axis, Plot, Figure, Matrix, Alignat, SubFigure, \
    NewPage, Command
import shutil
from pylatex.utils import bold, italic, NoEscape
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter, StrMethodFormatter
import matplotlib.font_manager
import rolling_7_days as rolling
from functions import trailing_90_days_graph, stacking_percentage_table, trailing_90_days_horizontal_graph,     create_final_dataframe, metric_calc, published_volume_filter, referral_highlights_filter, top_blog_graph_filters, segment_starts_per_visitor, trailing_90_days_graph_with_benchmark, trailing_90_days_graph_table, stacking_graph, metric_per_visit, stacking_percentage_helper, visit_count_per_post
warnings.filterwarnings("ignore")


pd.set_option('display.max_columns', None)

warnings.filterwarnings("ignore")

engine = snowflake_engine()

# ! pip3 install texlive-pictures --user






def run_python_function(brand, df, df_rolling_raw, df_video):

#     # Import data from 'data' folder
#     df = pd.read_csv(c.data_dir + brand + '_raw_data.npy', sep='\t')

#     # Change mparticle_id to correct datatype
#     try:
#         df.mparticle_id = df.mparticle_id.map(lambda x: '{:.0f}'.format(x))
#     except:
#         pass


#     # Import data from 'data' folder
#     df_rolling_raw = pd.read_csv(c.data_dir + 'rolling_raw_data.npy', sep='\t')





#     # Import data from 'data' folder
#     df_video = pd.read_csv(c.data_dir + 'video_raw_data.npy', sep='\t')

    # Filter to brand
    df = df[df.brand == brand]





    # Import data for Benchmarks
    df_benchmarks = pd.read_csv(c.data_dir + 'content_marketing_benchmarks.csv')





    # Create df for Site as a Whole
    df_site = df[['brand', 'week', 'visit_count_entire_site']].drop_duplicates()
    # df_site = df[['brand', 'date_day', 'week', 'date_month', 'visit_count_entire_site']].drop_duplicates()


    # # Adjust Metrics




    df = df[df.brand == brand]
    df_rolling_raw = df_rolling_raw[df_rolling_raw.brand == brand]
    df_video = df_video[df_video.brand == brand]
    df_benchmarks = df_benchmarks[df_benchmarks.brand == brand]
    df_site = df_site[df_site.brand == brand]





    # Create Date Fields
    today = datetime.now()
#     today = datetime.datetime.now()
    # today = today - timedelta(days=2) # Set Temp Today Date (to be deleted)
    last_saturday = today - timedelta(days=today.weekday()+2) #ToDo Change today to be the last saturday
    last_month_same_date = last_saturday - relativedelta(months=1)

    first_day_of_month = last_saturday.replace(day=1).strftime('%Y-%m-%d') 

    datetime_object = datetime.strptime(first_day_of_month, '%Y-%m-%d')
    first_day_of_previous_month = datetime_object - relativedelta(months=1)

    # Determine Sunday
    if last_saturday.weekday() == 6:
        sunday_current_week = last_saturday
    else: 
        sunday_current_week = last_saturday - timedelta(days=last_saturday.weekday()+1)

    sunday_previous_week = sunday_current_week - timedelta(days=7)
    ninety_days_ago = last_saturday - timedelta(days = 90)
    rolling_week_start = sunday_current_week - timedelta(days = 7)
    rolling_previous_week_start = rolling_week_start - timedelta(days=7)

    sunday_current_week = sunday_current_week.strftime('%Y-%m-%d')
    sunday_previous_week = sunday_previous_week.strftime('%Y-%m-%d')
    first_day_of_previous_month = first_day_of_previous_month.strftime('%Y-%m-%d')
    rolling_week_start = rolling_week_start.strftime('%Y-%m-%d')
    rolling_previous_week_start = rolling_previous_week_start.strftime('%Y-%m-%d')

    ninety_days_ago = ninety_days_ago.replace(hour=0, minute=0, second=0, microsecond=0)





    # Change Date Fields
    df['date_day'] = df['date_day'].astype('datetime64[ns]')
    df['publisheddate'] = df['publisheddate'].astype('datetime64[ns]')
    df['week'] = df['week'].astype('datetime64[ns]')
    df['published_week'] = df['published_week'].astype('datetime64[ns]')
    df['date_month'] = df['date_month'].astype('datetime64[ns]')
    df['published_date_month'] = df['published_date_month'].astype('datetime64[ns]')

    df_rolling_raw['published_week'] = df_rolling_raw['published_week'].astype('datetime64[ns]')

    df_video['week'] = df_video['week'].astype('datetime64[ns]')
    df_video['date_month'] = df_video['date_month'].astype('datetime64[ns]')
    df_video['date_day'] = df_video['date_day'].astype('datetime64[ns]')

    df_benchmarks['date_month'] = df_benchmarks['date_month'].astype('datetime64[ns]')

    # df_site['date_day'] = df_site['date_day'].astype('datetime64[ns]')
    df_site['week'] = df_site['week'].astype('datetime64[ns]')
    # df_site['date_month'] = df_site['date_month'].astype('datetime64[ns]')





    # Filter so Last Saturday is last day shown in tables
    df = df[df['date_day']<=last_saturday]
    df_video = df_video[df_video['date_day']<=last_saturday]





    # Filter Rolling
    df_rolling_raw = df_rolling_raw[df_rolling_raw['referral_platform']=='All']
    df_rolling_raw = df_rolling_raw[df_rolling_raw['bucket']=='7']





    # Create df for current week and current month
    df_week = df[df['week']==sunday_current_week]
    df_published_week = df[df['published_week']==sunday_current_week]
    df_video_week = df_video[df_video['week']==sunday_current_week]
    df_site_week = df_site[df_site['week']==sunday_current_week]

    df_previous_week = df[df['week']==sunday_previous_week]
    df_published_previous_week = df[df['published_week']==sunday_previous_week]
    df_video_previous_week = df_video[df_video['week']==sunday_previous_week]
    df_site_previous_week = df_site[df_site['week']==sunday_previous_week]

    df_month = df[df['date_month']==first_day_of_month]
    df_published_month = df[df['published_date_month']==first_day_of_month]
    df_video_month = df_video[df_video['date_month']==first_day_of_month]
    # df_site_month = df_site[df_site['date_month']==first_day_of_month]

    df_previous_month = df[df['date_month']==first_day_of_previous_month]
    df_published_previous_month = df[df['published_date_month']==first_day_of_previous_month]
    df_video_previous_month = df_video[df_video['date_month']==first_day_of_previous_month]
    # df_site_previous_month = df_site[df_site['date_month']==first_day_of_previous_month]

    # Max of last month be the same date as end of this month
    df_previous_month = df_previous_month[df_previous_month['date_day']<=last_month_same_date]
    df_published_previous_month = df_published_previous_month[df_published_previous_month['publisheddate']<=last_month_same_date]
    df_video_previous_month = df_video_previous_month[df_video_previous_month['date_day']<=last_month_same_date]
    # df_site_previous_month = df_site_previous_month[df_site_previous_month['date_day']<=last_month_same_date]

    df_rolling = df_rolling_raw[df_rolling_raw['published_week']==rolling_week_start]
    df_rolling_previous_week = df_rolling_raw[df_rolling_raw['published_week']==rolling_previous_week_start]





    final_table_column = ["title", "week", "week_over_week", "mtd", "month_over_month", "graph"]

    df_final = pd.DataFrame(columns = final_table_column)


    # # Calc Values

    # ### Section 1: Weekly Executive Summary - Visits




    # Calc Metrics
    visits_week, visits_week_over_week, visits_mtd, visits_month_over_month, visits_previous_week, visits_previous_month = metric_calc('count_distinct', 'visits_calc_id', df_week, df_previous_week, 
                                         df_month, df_previous_month)



    # Trailing 90 Days
    df_graph_temp = trailing_90_days_graph_table(df, df_benchmarks, ninety_days_ago, 'week', 'date_month', 
                                                 'visits_calc_id', 'weekly_visits')
    df_graph_temp

    # print(df_graph_temp)
    visits_graph = trailing_90_days_graph_with_benchmark(df_graph_temp, 'week', 'Visits', 'visits_calc_id', 
                                                              'weekly_visits', 'visits_graph.png', brand)
    # if brand != 'nbc' and brand != 'usa':
    #     visits_graph = trailing_90_days_graph(df_graph_temp, 'week', 'visits_calc_id', 'visits_graph.png', brand)
    # else:
    #     visits_graph = trailing_90_days_graph_with_benchmark(df_graph_temp, 'week', 'Visits', 'visits_calc_id', 
    #                                                          'weekly_visits', 'visits_graph.png', brand)








    df_final = create_final_dataframe(df_final, final_table_column, 'Visits', visits_week, visits_week_over_week,
                                      visits_mtd, visits_month_over_month, 'visits_graph.png')


    # ### Section 2: Weekly Executive Summary - Page Views per Visit




    page_views_per_visit_week, page_views_per_visit_week_over_week, page_views_per_visit_mtd,     page_views_per_visit_month_over_month = metric_per_visit('page_views',visits_week, visits_previous_week, 
                                                                 visits_mtd, visits_previous_month, df_week, 
                                                                 df_previous_week, df_month, df_previous_month, 'no')





    # Trailing 90 Days
    df_temp = df[df['week']>=ninety_days_ago]
    df_temp = df_temp.groupby('week').agg({'visits_calc_id': 'nunique', 'page_views': 'sum'}).reset_index()
    df_temp['page_views_per_visit'] =  df_temp['page_views'] / df_temp['visits_calc_id']
    df_temp = df_temp.drop(['visits_calc_id', 'page_views'], axis=1)
    df_temp['week'] = df_temp['week'].dt.strftime('%b %d')

    page_views_per_visits_graph = trailing_90_days_graph(df_temp, 'week', 'page_views_per_visit', 
                                                         'page_views_per_visits_graph.png', brand)





    df_final = create_final_dataframe(df_final, final_table_column, 'Page Views per Visit', 
                                      page_views_per_visit_week, page_views_per_visit_week_over_week, 
                                      page_views_per_visit_mtd, page_views_per_visit_month_over_month, 
                                      'page_views_per_visits_graph.png')


    # ### Section 3: Weekly Executive Summary - Published Volume




    # Filter on User Server and Content Type
    df_week_published_volume = published_volume_filter(df_published_week)
    df_previous_week_published_volume = published_volume_filter(df_published_previous_week)
    df_month_published_volume = published_volume_filter(df_published_month)
    df_previous_month_published_volume = published_volume_filter(df_published_previous_month)
    df_graph_published_volume = published_volume_filter(df)





    # Calc Metrics
    published_volume_week, published_volume_week_over_week, published_volume_mtd, published_volume_month_over_month,published_volume_previous_week, published_volume_previous_month=  metric_calc(
        'count_distinct', 'content_id', df_week_published_volume, df_previous_week_published_volume, 
        df_month_published_volume,  df_previous_month_published_volume)





    # Trailing 90 Days
    df_graph_temp = trailing_90_days_graph_table(df, df_benchmarks, ninety_days_ago, 'published_week', 
                                                 'published_date_month', 'content_id', 'number_of_posts_published')

    published_volume_graph = trailing_90_days_graph(df_graph_temp, 'published_week', 'content_id', 
                                                         'published_volumne_graph.png', brand)

    # if brand != 'bravo':
    #     published_volume_graph = trailing_90_days_graph_with_benchmark(df_graph_temp, 'published_week', 
    #                                                                    'Publsished Volume', 'content_id', 
    #                                                                    'number_of_posts_published',
    #                                                                    'published_volumne_graph.png', brand)
    # else: 
    #     published_volume_graph = trailing_90_days_graph(df_graph_temp, 'published_week', 'content_id', 
    #                                                      'published_volumne_graph.png', brand)





    df_final = create_final_dataframe(df_final, final_table_column, 'Published Volume', published_volume_week, 
                                      published_volume_week_over_week, published_volume_mtd, 
                                      published_volume_month_over_month, 'published_volumne_graph.png')


    ### Section 4: Weekly Executive Summary - Rolling Visits per Post 7 Day Bucket



    # Current Week
    rolling_visit_count = df_rolling['visit_count'].sum()
    rolling_page_count = df_rolling['page_count'].sum()
    rolling_week = rolling_visit_count / rolling_page_count

    # Week over Week
    rolling_visit_count_previous_week = df_rolling_previous_week['visit_count'].sum()
    rolling_page_count_previous_week = df_rolling_previous_week['page_count'].sum()
    rolling_week_previous_week = rolling_visit_count_previous_week / rolling_page_count_previous_week
    rolling_week_week_over_week = (rolling_week - rolling_week_previous_week)/rolling_week_previous_week


    # Trailing 90 Days
    df_temp = df_rolling_raw[df_rolling_raw['published_week']>=ninety_days_ago]
    df_temp = df_temp.groupby('published_week').agg({'visit_count': 'sum', 'page_count': 'sum'}).reset_index()
    df_temp['rolling_visits_per_post'] = df_temp['visit_count'] / df_temp['page_count']
    df_temp = df_temp.drop(['visit_count', 'page_count'], axis=1)
    df_temp['published_week'] = df_temp['published_week'].dt.strftime('%b %d')

    rolling_visits_per_post_graph = trailing_90_days_graph(df_temp, 'published_week', 
                                                           'rolling_visits_per_post', 
                                                           'rolling_visits_per_post_graph.png', brand)


    df_final = create_final_dataframe(df_final, final_table_column, 'Rolling Visits per Post 7 Day Bucket', 
                                      rolling_week, rolling_week_week_over_week, -100, -100,
                                      'rolling_visits_per_post_graph.png')



    ### Section 5: Weekly Executive Summary - Visit Count by Referral Sources




    referral_source = ['Direct', 'Other', 'Search', 'Social']
    df_temp = stacking_percentage_table(df, 'referral_source', 'visits_calc_id', referral_source, ninety_days_ago)





    visit_count_by_referral_sources_graph = stacking_graph(df_temp, 'Week', 'Visit Count by Referral Sources', 
                                                           referral_source,
                                                           'visit_count_by_referral_sources_graph.png', brand)





    df_final = create_final_dataframe(df_final, final_table_column,'Visit Count by Referral Sources', -100, -100, 
                                      -100, -100, 'visit_count_by_referral_sources_graph.png')


    ### Section 6: Weekly Executive Summary - New vs Returning Visitors



    new_and_returning_user = ['New', 'Returning']
    df_temp = stacking_percentage_table(df, 'new_or_returning_user', 'visitor_id', new_and_returning_user, 
                                        ninety_days_ago)

    new_vs_returning_visitors_graph = stacking_graph(df_temp, 'Week', 'New vs Returning Visitors', 
                                                     new_and_returning_user, 'new_vs_returning_visitors_graph.png', 
                                                     brand)

    df_final = create_final_dataframe(df_final, final_table_column, 'New vs Returning Visitors', -100, -100, -100, 
                                      -100, 'new_vs_returning_visitors_graph.png')

    # ### Section 7: Editorial Referral Highlights - Visit Count by Search Referrals




    df_week_temp = referral_highlights_filter(df_week, 'Search')
    df_previous_week_temp = referral_highlights_filter(df_previous_week, 'Search')
    df_month_temp = referral_highlights_filter(df_month, 'Search')
    df_previous_month_temp = referral_highlights_filter(df_previous_month, 'Search')





    # Calc Metrics
    visits_by_search_week, visits_by_search_week_over_week, visits_by_search_mtd, visits_by_search_month_over_month, visits_by_search_previous_week, visits_by_search_previous_month=  metric_calc(
        'count_distinct', 'visits_calc_id', df_week_temp, df_previous_week_temp, df_month_temp, 
        df_previous_month_temp)





    # Trailing 90 Days
    df_temp = df[df['page_name_filter']=='Keep']
    df_temp = df_temp[df_temp['referral_source']=='Search']
    df_temp = df_temp[df_temp['week']>=ninety_days_ago]
    df_temp = df_temp.groupby(['week'])['visits_calc_id'].nunique()
    df_temp = df_temp.reset_index()
    df_temp['week'] = df_temp['week'].dt.strftime('%b %d')

    visits_by_search_referrals_graph = trailing_90_days_graph(df_temp, 'week', 'visits_calc_id', 
                                                              'visits_by_search_referrals_graph.png', brand)





    df_final = create_final_dataframe(df_final, final_table_column, 'Visit Count by Search Referrals', 
                                      visits_by_search_week, visits_by_search_week_over_week, visits_by_search_mtd, 
                                      visits_by_search_month_over_month, 
                                      'visits_by_search_referrals_graph.png')


    # ### Section 8: Editorial Referral Highlights - Visit Count per Post by Search Referrals




    visits_per_post_by_search_week, visits_per_post_by_search_week_over_week,             visits_per_post_by_search_mtd, visits_per_post_by_search_month_over_month = visit_count_per_post(
        visits_by_search_week, visits_by_search_previous_week, visits_by_search_mtd, visits_by_search_previous_month, 
        df_week_temp, df_previous_week_temp, df_month_temp)





    # Trailing 90 Days
    df_temp = df[df['page_name_filter']=='Keep']
    df_temp = df_temp[df_temp['referral_source']=='Search']
    df_temp = df_temp[df_temp['week']>=ninety_days_ago]
    df_temp = df_temp.groupby('week').agg({'visits_calc_id': 'nunique', 'content_id': 'nunique'}).reset_index()
    df_temp['visits_per_post_by_search'] = df_temp['visits_calc_id'] / df_temp['content_id']
    df_temp = df_temp.drop(['visits_calc_id', 'content_id'], axis=1)
    df_temp = df_temp.reset_index()
    df_temp['week'] = df_temp['week'].dt.strftime('%b %d')

    visits_per_post_by_search_graph = trailing_90_days_graph(df_temp, 'week', 
                                                             'visits_per_post_by_search', 
                                                             'visits_per_post_by_search_graph.png', brand)





    df_final = create_final_dataframe(df_final, final_table_column, 
                                      'Visit Count per Post by Search Referrals', visits_per_post_by_search_week, 
                                      visits_per_post_by_search_week_over_week,
                                      visits_per_post_by_search_mtd, visits_per_post_by_search_month_over_month, 
                                      'visits_per_post_by_search_graph.png')


    # ### Section 9: Editorial Referral Highlights - Top Blog Post by Visit Count by Search Referrals




    # Trailing 90 Days
    df_temp = top_blog_graph_filters(df_week, 'Search', ninety_days_ago)

    # df_temp

    top_blog_post_by_visit_count_search_referral_graph = trailing_90_days_horizontal_graph(
        df_temp, 'pagename', 'visits_calc_id', 'top_blog_post_by_visit_count_search_referral.png', brand)





    df_final = create_final_dataframe(df_final, final_table_column, 
                                      'Top Blog Post by Visit Count by Search Referrals', -100, -100, -100, -100,
                                      'top_blog_post_by_visit_count_search_referral.png')


    # ### Section 10: Editorial Referral Highlights - Visit Count by Social Referrals




    df_week_temp = referral_highlights_filter(df_week, 'Social')
    df_previous_week_temp = referral_highlights_filter(df_previous_week, 'Social')
    df_month_temp = referral_highlights_filter(df_month, 'Social')
    df_previous_month_temp = referral_highlights_filter(df_previous_month, 'Social')





    # Calc Metrics
    visits_by_social_week, visits_by_social_week_over_week, visits_by_social_mtd, visits_by_social_month_over_month,visits_by_social_previous_week, visits_by_social_previous_month =  metric_calc(
        'count_distinct', 'visits_calc_id', df_week_temp, df_previous_week_temp, df_month_temp, 
        df_previous_month_temp)





    # Trailing 90 Days
    df_temp = df[df['page_name_filter']=='Keep']
    df_temp = df_temp[df_temp['referral_source']=='Social']
    df_temp = df_temp[df_temp['week']>=ninety_days_ago]
    df_temp = df_temp.groupby(['week'])['visits_calc_id'].nunique()
    df_temp = df_temp.reset_index()
    df_temp['week'] = df_temp['week'].dt.strftime('%b %d')

    visits_by_social_referrals_graph = trailing_90_days_graph(df_temp, 'week', 'visits_calc_id', 
                                                              'visits_by_social_referrals_graph.png', brand)





    df_final = create_final_dataframe(df_final, final_table_column, 'Visit Count by Social Referrals', 
                                      visits_by_social_week, visits_by_social_week_over_week, 
                                      visits_by_social_mtd, visits_by_social_month_over_month,
                                      'visits_by_social_referrals_graph.png')


    # ### Section 11: Editorial Referral Highlights - Visit Count per Post by Social Referrals




    visits_per_post_by_social_week, visits_per_post_by_social_week_over_week,             visits_per_post_by_social_mtd, visits_per_post_by_social_month_over_month = visit_count_per_post(
        visits_by_social_week, visits_by_social_previous_week, visits_by_social_mtd, visits_by_social_previous_month, 
        df_week_temp, df_previous_week_temp, df_month_temp)





    # Trailing 90 Days
    df_temp = df[df['page_name_filter']=='Keep']
    df_temp = df_temp[df_temp['referral_source']=='Social']
    df_temp = df_temp[df_temp['week']>=ninety_days_ago]
    df_temp = df_temp.groupby('week').agg({'visits_calc_id': 'nunique', 'content_id': 'nunique'}).reset_index()
    df_temp['visits_per_post_by_social'] = df_temp['visits_calc_id'] / df_temp['content_id']
    df_temp = df_temp.drop(['visits_calc_id', 'content_id'], axis=1)
    df_temp = df_temp.reset_index()
    df_temp['week'] = df_temp['week'].dt.strftime('%b %d')

    visits_per_post_by_social_graph = trailing_90_days_graph(df_temp, 'week', 'visits_per_post_by_social', 
                                                             'visits_per_post_by_social_graph.png', brand)





    df_final = create_final_dataframe(df_final, final_table_column, 
                                      'Visit Count per Post by Social Referrals', visits_per_post_by_social_week,
                                      visits_per_post_by_social_week_over_week,
                                      visits_per_post_by_social_mtd, visits_per_post_by_social_month_over_month,
                                      'visits_per_post_by_social_graph.png')


    # ### Section 12: Editorial Referral Highlights - Top Blog Post by Visit Count by Social Referrals




    # Trailing 90 Days
    df_temp = top_blog_graph_filters(df_week, 'Social', ninety_days_ago)

    top_blog_post_by_visit_count_social_referral_graph = trailing_90_days_horizontal_graph(
        df_temp, 'pagename', 'visits_calc_id', 'top_blog_post_by_visit_count_social_referral.png', brand)





    df_final = create_final_dataframe(df_final, final_table_column, 
                                      'Top Blog Post by Visit Count by Social Referrals', -100, -100, -100, -100,
                                      'top_blog_post_by_visit_count_social_referral.png')
    
    
    try:
        # ### Section 13: FEP Segment Starts

        fep_week, fep_week_over_week, fep_mtd, fep_month_over_month,fep_previous_week, fep_previous_month = metric_calc(
            'sum', 'fep_starts', df_video_week, df_video_previous_week, df_video_month, 
            df_video_previous_month)





        # Trailing 90 Days
        df_temp = df_video[df_video['week']>=ninety_days_ago]
        df_temp = df_temp.groupby(['week'])['fep_starts'].sum()
        df_temp = df_temp.reset_index()
        df_temp['week'] = df_temp['week'].dt.strftime('%b %d')
        df_temp['week'] = df_temp['week'].astype(str)

        fep_segment_starts_graph = trailing_90_days_graph(df_temp, 'week', 'fep_starts', 
                                                          'fep_segment_starts_graph.png', brand)





        df_final = create_final_dataframe(df_final, final_table_column, 
                                          'FEP Segment Starts', fep_week, fep_week_over_week, fep_mtd, 
                                          fep_month_over_month, 'fep_segment_starts_graph.png')


        # ### Section 14: FEP Segment Starts per Visitors




        fep_per_visitor_week, fep_per_visitor_week_over_week, fep_per_visitor_mtd, fep_per_visitor_month_over_month, df_temp = segment_starts_per_visitor(fep_week, fep_mtd, ninety_days_ago, df_video_week, df_video_previous_week, 
                                             df_video_month, df_video_previous_month, df_video, 'fep_starts')

        fep_per_visitor_graph = trailing_90_days_graph(df_temp, 'week', 'fep_starts_per_visitor', 
                                                       'fep_per_visitor_graph.png', brand)





        df_final = create_final_dataframe(df_final, final_table_column, 
                                          'FEP Video Starts per Visitor', fep_per_visitor_week, 
                                          fep_per_visitor_week_over_week, fep_per_visitor_mtd, 
                                          fep_per_visitor_month_over_month, 'fep_per_visitor_graph.png')
    
    except:
        pass


    # ### Section 15: SFV Segment Starts




    sfv_week, sfv_week_over_week, sfv_mtd, sfv_month_over_month,sfv_previous_week, sfv_previous_month = metric_calc(
        'sum', 'sfv_starts', df_video_week, df_video_previous_week, df_video_month, 
        df_video_previous_month)





    # Trailing 90 Days
    df_temp = df_video[df_video['week']>=ninety_days_ago]
    df_temp = df_temp.groupby(['week'])['sfv_starts'].sum()
    df_temp = df_temp.reset_index()
    df_temp['week'] = df_temp['week'].dt.strftime('%b %d')
    df_temp['week'] = df_temp['week'].astype(str)

    sfv_segment_starts_graph = trailing_90_days_graph(df_temp, 'week', 'sfv_starts', 
                                                      'sfv_segment_starts_graph.png', brand)





    df_final = create_final_dataframe(df_final, final_table_column, 
                                      'SFV Segment Starts', sfv_week, sfv_week_over_week, sfv_mtd, 
                                      sfv_month_over_month, 'sfv_segment_starts_graph.png')


    # ### Section 16: SFV Segment Starts per Visitors




    sfv_per_visitor_week, sfv_per_visitor_week_over_week, sfv_per_visitor_mtd, sfv_per_visitor_month_over_month, df_temp = segment_starts_per_visitor(sfv_week, sfv_mtd, ninety_days_ago, df_video_week, df_video_previous_week, 
                                         df_video_month, df_video_previous_month, df_video, 'sfv_starts')

    sfv_per_visitor_graph = trailing_90_days_graph(df_temp, 'week', 'sfv_starts_per_visitor', 
                                                   'sfv_per_visitor_graph.png', brand)





    df_final = create_final_dataframe(df_final, final_table_column, 
                                      'SFV Video Starts per Visitor', sfv_per_visitor_week, 
                                      sfv_per_visitor_week_over_week, sfv_per_visitor_mtd, 
                                      sfv_per_visitor_month_over_month, 'sfv_per_visitor_graph.png')


    # ### Section 17: Share of .com Entries Through Content Marketing




    # Setting up Table for Graph

    # Site Visit Count by Week
    df_site_temp = df_site[df_site['week']>=ninety_days_ago]
    df_site_temp = df_site_temp.groupby(by='week')['visit_count_entire_site'].sum()
    df_site_temp = df_site_temp.reset_index()

    # Content Marketing Visit Count by Week
    df_content_temp = df[df['week']>=ninety_days_ago]
    df_content_temp = df_content_temp.groupby(by='week')['visits_calc_id'].nunique()
    df_content_temp = df_content_temp.reset_index()


    # Combined
    df_content_temp = pd.merge(df_site_temp, df_content_temp, on=["week"])
    df_content_temp.rename(columns={'visit_count_entire_site': 'total', 
                                    'visits_calc_id': 'Editorial'}, inplace=True)
    df_content_temp['Non-Editorial'] = df_content_temp['total'] - df_content_temp['Editorial']
    legend_list = ['Editorial', 'Non-Editorial']


    df_temp = stacking_percentage_helper(legend_list, df_content_temp)
    df_temp['week'] = df_temp['week'].dt.strftime('%b %d')





    share_of_com_entries_graph = stacking_graph(df_temp, 'Week', 'Share of .com Entries', legend_list, 
                                                     'share_of_com_entries_graph.png', brand)




    df_final = create_final_dataframe(df_final, final_table_column, 'Share of .com Entries', -100, -100, -100, 
                                      -100, 'share_of_com_entries_graph.png')


    # ### Output




    # Output final to data folder
    df_final.to_csv(c.output_dir + brand + '/' + 'final_directory.npy', sep='\t', index=False)
    df_final
    
    return








# PDF

def create_pdf(brand_title):

    # Brand
    # brand_title = 'USA' #USA SyFy Oxygen Bravo NBC
    brand = brand_title.lower()

    # Create Date Fields
    today = datetime.now()
    # today = today - timedelta(days=10) # Set Temp Today Date (to be deleted)
    last_saturday = today - timedelta(days=today.weekday()+2) #ToDo Change today to be the last saturday
    today_date = today.strftime("%B %d, %Y")
    last_saturday_date = last_saturday.strftime("%B %d, %Y")

    # Determined Sunday
    if last_saturday.weekday() == 6:
        week_date = last_saturday_date.copy()
    else:
        sunday = last_saturday - timedelta(days=last_saturday.weekday() + 1)
        week_date = sunday.strftime("%B %d, %Y")

    # Directory
    df_directory = pd.read_csv(c.output_dir + brand + '/' + 'final_directory.npy', sep='\t')

    # Convert to float
    df_directory['week'] = df_directory['week'].astype(float)
    df_directory['week_over_week'] = df_directory['week_over_week'].astype(float)
    df_directory['mtd'] = df_directory['mtd'].astype(float)
    df_directory['month_over_month'] = df_directory['month_over_month'].astype(float)

    # Set Title as index
    df_directory.set_index('title', inplace=True)

    # Create Dict
    directory_dict =df_directory.to_dict('index')

    # Monthly Benchmarks
    month_benchmark_dict = {
        'Bravo': 13400000,
        'USA': 746852,
        'NBC': 4114637,
        'SyFy': 0,
        'Oxygen': 0,
    }
    month_benchmark = month_benchmark_dict[brand_title]
    try:
        visits_pacing_to_goal = directory_dict['Visits']['mtd'] / month_benchmark
    except:
        visits_pacing_to_goal = 0








    # Create PDF
    ############################################################################################
    ###  Intro
    ############################################################################################

    #the Font might not be working
    doc = Document(fontenc = 'Rock Sans') # geometry_options=geometry_options geometry_options = {"margin": "0.7in"}#right=0.5in,
    # doc = Document('basic',font_size = '', inputenc = 'utf8x', lmodern = False, fontenc = None, textcomp = None)

    doc.preamble.append(NoEscape(r'''\usepackage[legalpaper, landscape, left=0.5in, right=0.5in, top=0.1in, 
                                    bottom=0in]{geometry}'''))

    # Add document header
    header = PageStyle("header")

    doc.preamble.append(header)
    doc.change_document_style("header")

    # Generating first page style
    first_page = PageStyle("firstpage")
    second_page = PageStyle("secondpage")
    third_page = PageStyle("thirdpage")


    ############################################################################################
    ###  Page 1
    ############################################################################################


    # Add Heading
    with doc.create(MiniPage(align='c')):
        doc.append(LargeText(bold(brand_title + " Weekly Executive Summary")))
        doc.append(LineBreak())
        doc.append(SmallText(bold("Week of: " + week_date + " | Report Date: " + today_date)))

    doc.append(LineBreak())
    doc.append(LineBreak())
    doc.append(LineBreak())


    doc = create_card_mtd_month_projected('Visits', 
                                          "{:,.0f}".format(directory_dict['Visits']['week']), 
                                    "{0:.0%}".format(directory_dict['Visits']['week_over_week']),
                                    "{:,.0f}".format(directory_dict['Visits']['mtd']), 
                                    "{0:.0%}".format(directory_dict['Visits']['month_over_month']),
                                          "{0:.0%}".format(visits_pacing_to_goal),
                                    c.output_dir + brand + '/' + directory_dict['Visits']['graph'], doc)

    doc = create_card_rolling('Rolling Visits Per Post', 
                        "{:,.0f}".format(directory_dict['Rolling Visits per Post 7 Day Bucket']['week']), 
                        "{0:.0%}".format(directory_dict['Rolling Visits per Post 7 Day Bucket']['week_over_week']),
                        c.output_dir + brand + '/' + directory_dict['Rolling Visits per Post 7 Day Bucket']['graph'], 
                              doc)

    doc.append(LineBreak())
    doc.append(LineBreak())
    doc.append(LineBreak())
    doc.append(LineBreak())

    doc = create_card_mtd_page_1('Page Views per Visit', 
                                 "{:,.2f}".format(directory_dict['Page Views per Visit']['week']), 
                    "{0:.2%}".format(directory_dict['Page Views per Visit']['week_over_week']),
                    "{:,.2f}".format(directory_dict['Page Views per Visit']['mtd']), 
                    "{0:.2%}".format(directory_dict['Page Views per Visit']['month_over_month']),
                    c.output_dir + brand + '/' + directory_dict['Page Views per Visit']['graph'], doc)

    doc = create_card_no_metrics_stacked_graph('New vs Returning Visitors', 
                           c.output_dir + brand + '/' + directory_dict['New vs Returning Visitors']['graph'], doc)


    doc.append(LineBreak())
    doc.append(LineBreak())
    doc.append(LineBreak())
    doc.append(LineBreak())


    doc = create_card_mtd_page_1('Published Volume', "{:,.0f}".format(directory_dict['Published Volume']['week']),
                    "{0:.2%}".format(directory_dict['Published Volume']['week_over_week']),
                    "{:,.0f}".format(directory_dict['Published Volume']['mtd']), 
                    "{0:.2%}".format(directory_dict['Published Volume']['month_over_month']),
                    c.output_dir + brand + '/' + directory_dict['Published Volume']['graph'], doc)

    doc = create_card_no_metrics_stacked_graph('Visit Count by Referral Sources', 
                           c.output_dir + brand + '/' + directory_dict['Visit Count by Referral Sources']['graph'], 
                                               doc)



    doc.preamble.append(first_page)
    doc.append(NewPage())
    # End first page style



    ############################################################################################
    ###  Page 2
    ############################################################################################

    # Add Heading
    with doc.create(MiniPage(align='c')):
        doc.append(LargeText(bold(brand_title + " Weekly Executive Summary")))
        doc.append(LineBreak())
        doc.append(SmallText(bold("Week of: " + week_date + " | Report Date: " + today_date)))

    doc.append(LineBreak())
    doc.append(LineBreak())
    doc.append(LineBreak())
    doc.append(LineBreak())


    doc = create_card_mtd('Search Visit Count', 
                    "{:,.0f}".format(directory_dict['Visit Count by Search Referrals']['week']),
                    "{0:.0%}".format(directory_dict['Visit Count by Search Referrals']['week_over_week']),
                    "{:,.0f}".format(directory_dict['Visit Count by Search Referrals']['mtd']),
                    "{0:.0%}".format(directory_dict['Visit Count by Search Referrals']['month_over_month']),
                    c.output_dir + brand + '/' + directory_dict['Visit Count by Search Referrals']['graph'], doc)

    doc = create_card_mtd('Social Visit Count', 
                    "{:,.0f}".format(directory_dict['Visit Count by Social Referrals']['week']),
                    "{0:.0%}".format(directory_dict['Visit Count by Social Referrals']['week_over_week']),
                    "{:,.0f}".format(directory_dict['Visit Count by Social Referrals']['mtd']), 
                    "{0:.0%}".format(directory_dict['Visit Count by Social Referrals']['month_over_month']),
                    c.output_dir + brand + '/' + directory_dict['Visit Count by Social Referrals']['graph'], doc)

    doc.append(LineBreak()) 
    doc.append(LineBreak())
    doc.append(LineBreak())
    doc.append(LineBreak())

    doc = create_card_mtd('Search Visit Count per Post', 
                    "{:,.0f}".format(directory_dict['Visit Count per Post by Search Referrals']['week']), 
                    "{0:.0%}".format(directory_dict['Visit Count per Post by Search Referrals']['week_over_week']),
                    "{:,.2f}".format(directory_dict['Visit Count per Post by Search Referrals']['mtd']), 
                    "{0:.0%}".format(directory_dict['Visit Count per Post by Search Referrals']['month_over_month']),
                    c.output_dir + brand + '/' + directory_dict['Visit Count per Post by Search Referrals']['graph'], 
                          doc)

    doc = create_card_mtd('Social Visit Count per Post', 
                    "{:,.0f}".format(directory_dict['Visit Count per Post by Social Referrals']['week']), 
                    "{0:.0%}".format(directory_dict['Visit Count per Post by Social Referrals']['week_over_week']),
                    "{:,.2f}".format(directory_dict['Visit Count per Post by Social Referrals']['mtd']), 
                    "{0:.0%}".format(directory_dict['Visit Count per Post by Social Referrals']['month_over_month']),
                    c.output_dir + brand + '/' + directory_dict['Visit Count per Post by Social Referrals']['graph'], 
                          doc)

    doc.append(LineBreak()) 
    doc.append(LineBreak())
    doc.append(LineBreak())
    doc.append(LineBreak())

    doc = create_card_no_metrics('Search Top Blog Post by Visit Count', 
                c.output_dir + brand + '/' + directory_dict['Top Blog Post by Visit Count by Search Referrals']['graph'], 
                                 doc)

    doc = create_card_no_metrics('Social Top Blog Post by Visit Count', 
                c.output_dir + brand + '/' + directory_dict['Top Blog Post by Visit Count by Social Referrals']['graph'], 
                                 doc)

    doc.preamble.append(second_page)
    doc.append(NewPage())
    # End of Second Page


    if brand_title == 'USA' or brand_title == 'NBC':
        ############################################################################################
        ###  Page 3
        ############################################################################################

        # Add Heading
        with doc.create(MiniPage(align='c')):
            doc.append(LargeText(bold(brand_title + " Weekly Executive Summary")))
            doc.append(LineBreak())
            doc.append(SmallText(bold("Week of: " + week_date + " | Report Date: " + today_date)))

        doc.append(LineBreak())
        doc.append(LineBreak())
        doc.append(LineBreak())
        doc.append(LineBreak())


        # doc = create_card_mtd('FEP Segment Starts', 
        #                 "{:,.0f}".format(directory_dict['FEP Segment Starts']['week']),
        #                 "{0:.0%}".format(directory_dict['FEP Segment Starts']['week_over_week']),
        #                 "{:,.0f}".format(directory_dict['FEP Segment Starts']['mtd']),
        #                 "{0:.0%}".format(directory_dict['FEP Segment Starts']['month_over_month']),
        #                 c.output_dir + brand + '/' + directory_dict['FEP Segment Starts']['graph'], doc)

        doc = create_card_mtd('SFV Segment Starts', 
                        "{:,.0f}".format(directory_dict['SFV Segment Starts']['week']),
                        "{0:.0%}".format(directory_dict['SFV Segment Starts']['week_over_week']),
                        "{:,.0f}".format(directory_dict['SFV Segment Starts']['mtd']),
                        "{0:.0%}".format(directory_dict['SFV Segment Starts']['month_over_month']),
                        c.output_dir + brand + '/' + directory_dict['SFV Segment Starts']['graph'], doc)

        # doc.append(LineBreak()) 

        # doc = create_card_mtd('FEP Video Starts per Visitor', 
        #                 "{:,.2f}".format(directory_dict['FEP Video Starts per Visitor']['week']), 
        #                 "{0:.2%}".format(directory_dict['FEP Video Starts per Visitor']['week_over_week']),
        #                 "{:,.2f}".format(directory_dict['FEP Video Starts per Visitor']['mtd']), 
        #                 "{0:.2%}".format(directory_dict['FEP Video Starts per Visitor']['month_over_month']),
        #                 c.output_dir + brand + '/' + directory_dict['FEP Video Starts per Visitor']['graph'], doc)


        doc = create_card_mtd('SFV Video Starts per Visitor', 
                        "{:,.2f}".format(directory_dict['SFV Video Starts per Visitor']['week']), 
                        "{0:.2%}".format(directory_dict['SFV Video Starts per Visitor']['week_over_week']),
                        "{:,.2f}".format(directory_dict['SFV Video Starts per Visitor']['mtd']), 
                        "{0:.2%}".format(directory_dict['SFV Video Starts per Visitor']['month_over_month']),
                        c.output_dir + brand + '/' + directory_dict['SFV Video Starts per Visitor']['graph'], doc)

        doc.append(LineBreak()) 
        doc.append(LineBreak()) 
        doc.append(LineBreak()) 
        doc.append(LineBreak())

        doc = create_card_no_metrics_stacked_graph('Share of .com Entries', 
                           c.output_dir + brand + '/' + directory_dict['Share of .com Entries']['graph'], doc)


        doc.preamble.append(third_page)
        doc.append(NewPage())
        # End of Second Page

    output_file_name = brand_title + " -- " + week_date + " -- Content_Marketing_Executive_Summary"
    try:
        doc.generate_pdf(output_file_name, clean_tex=True)
    except: 
        pass


    # Move Output files
    try:
        origin = c.root_dir + output_file_name + '.pdf'
        target = c.root_dir + 'pdf/' + output_file_name + '.pdf'
        shutil.move(origin, target)
    except:
        pass

    file_ending = ['.aux', '.log', '.tex']
    for i in file_ending: 
        try:
            origin = c.root_dir + output_file_name + i
            target = c.root_dir + 'Old/' + output_file_name + i
            shutil.move(origin, target)
        except:
            pass
    return




#### PAGE 1

def create_card_mtd_page_1(section_name, metric_week, week_over_week, mtd, month_over_month, graph, doc):

    with doc.create(MiniPage(width=NoEscape(r"0.4\linewidth"))):
        with doc.create(Section(section_name + ': ' + str(metric_week) + ' (WoW ' + str(week_over_week) + ')', 
                                numbering= False)):
            with doc.create(MiniPage(width=NoEscape('120px'))):
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), align='l', pos='t')):
                    doc.append(bold(' MTD: '))
                    doc.append(mtd)
                doc.append(LineBreak())
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), height=NoEscape(r"0.9\textwidth"),
                                     align='l', pos='t')):
                    doc.append(bold(' MoM: '))
                    doc.append(month_over_month)

            with doc.create(
                SubFigure(position='c', width=NoEscape('210px'))) as right_imagesRow1:
                right_imagesRow1.add_image(graph, width=NoEscape(r'0.95\linewidth'))
    
    return doc

def create_card_mtd_month_projected(section_name, metric_week, week_over_week, mtd, month_over_month, pacing, graph, doc):
    with doc.create(MiniPage(width=NoEscape(r"0.4\linewidth"))):
        with doc.create(Section(section_name + ': ' + str(metric_week)+ ' (WoW ' + str(week_over_week) + ')',
                               numbering= False)):
            with doc.create(MiniPage(width=NoEscape('120px'))):
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), align='l', pos='t')):
                    doc.append(bold(' MTD: '))
                    doc.append(mtd)
                doc.append(LineBreak())
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"),
                                     align='l', pos='t')):
                    doc.append(bold(' MoM: '))
                    doc.append(month_over_month)
                doc.append(LineBreak())
                if pacing != 0:
                    with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), height=NoEscape(r"0.8\textwidth"),
                                         align='l', pos='t')):
                        doc.append(bold(' Pacing to Goal: '))
                        doc.append(pacing)

            with doc.create(
                SubFigure(position='c', width=NoEscape('210px'))) as right_imagesRow1: # 210
                right_imagesRow1.add_image(graph, width=NoEscape(r'0.95\linewidth'))
    
    return doc

def create_card_rolling(section_name, metric_week, week_over_week, graph, doc):
    with doc.create(MiniPage(width=NoEscape(r"0.4\linewidth"))):
        with doc.create(Section(section_name + ': ' + str(metric_week) + ' (WoW ' + str(week_over_week) + ')',
                                numbering= False)):
            with doc.create(
                SubFigure(position='r', width=NoEscape('210px'))) as right_imagesRow1:
                right_imagesRow1.add_image(graph, width=NoEscape(r'0.95\linewidth'))
    
    return doc



#### PAGE 2 and 3


def create_card_mtd(section_name, metric_week, week_over_week, mtd, month_over_month, graph, doc):
    with doc.create(MiniPage(width=NoEscape(r"0.5\linewidth"))):
        with doc.create(Section(
            section_name + ': ' + str(metric_week) + ' (WoW ' + str(week_over_week) + ')', numbering= False)):
            with doc.create(MiniPage(width=NoEscape('120px'))):
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), align='l', pos='t')):
                    doc.append(bold(' MTD: '))
                    doc.append(mtd)
                doc.append(LineBreak())
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), height=NoEscape(r"0.9\textwidth"),
                                     align='l', pos='t')):
                    doc.append(bold(' MoM: '))
                    doc.append(month_over_month)

            with doc.create(
                SubFigure(position='c', width=NoEscape('210px'))) as right_imagesRow1:
                right_imagesRow1.add_image(graph, width=NoEscape(r'0.95\linewidth'))
    
    return doc


def create_card_no_metrics(section_name, graph, doc):
    with doc.create(MiniPage(width=NoEscape(r"0.5\linewidth"))):
        with doc.create(Section(section_name, numbering= False)):
            with doc.create(
                SubFigure(position='c', width=NoEscape(r"1\textwidth")
                         )) as right_imagesRow1:
                right_imagesRow1.add_image(graph, width=NoEscape(r'1\linewidth'))
    
    return doc


def create_card_no_metrics_stacked_graph(section_name, graph, doc):
    with doc.create(MiniPage(width=NoEscape(r"0.5\linewidth"))):
        with doc.create(Section(section_name, numbering= False)):
            with doc.create(
                SubFigure(position='c', width=NoEscape('190px')
                         )) as right_imagesRow1:
                right_imagesRow1.add_image(graph, width=NoEscape(r'1\linewidth'))
    
    return doc







