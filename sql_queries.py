from util import *



def run_import_sql(engine, column_names, service_account):
    query_name = """Main SQL Run"""
        
        
        
    query = """
    
    //drop table workspace_al.page_name_nbc;
    //create table workspace_al.page_name_nbc as
    with page_name_nbc as
    (
      -- Determine PageName
      select
        pageName
        , CONTENTID
      from
      (
        select 
          COALESCE(pagename, articletitle) as pageName
          , CONTENTID
          , count(distinct VISITS_CALC_ID) as visit_name
          , RANK() OVER(PARTITION BY CONTENTID ORDER BY visit_name DESC) content_rank
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_NBC_COM_BLOG"
        where Page_Views != 0
            and pageurl is not null
            and custom_sub_section != 'Unspecified'
            and date_time >= current_date - 14
        group by 1,2
      )
      where content_rank = 1
    )
    //;
    ,



    //drop table workspace_al.page_name_usa;
    //create table workspace_al.page_name_usa as
    page_name_usa as
    (
      -- Determine PageName
      select
        pageName
        , CONTENTID
      from
      (
        select 
          COALESCE(pagename, articletitle) as pageName
          , CONTENTID
          , count(distinct VISITS_CALC_ID) as visit_name
          , RANK() OVER(PARTITION BY CONTENTID ORDER BY visit_name DESC) content_rank
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_USA_NETWORK_COM_BLOG"
        where Page_Views != 0
            and pageurl is not null
            and custom_sub_section != 'Unspecified'
            and date_time >= current_date - 14
        group by 1,2
      )
      where content_rank = 1
    )
    //;
    ,




    //drop table workspace_al.page_name_bravo;
    //create table workspace_al.page_name_bravo as
    page_name_bravo as
    (
      -- Determine PageName
      select
        pageName
        , CONTENTID
      from
      (
        select 
          COALESCE(pagename, articletitle) as pageName
          , CONTENTID
          , count(distinct VISITS_CALC_ID) as visit_name
          , RANK() OVER(PARTITION BY CONTENTID ORDER BY visit_name DESC) content_rank
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_BRAVO_TV_BLOG"
        where Page_Views != 0
            and pageurl is not null
            and custom_sub_section != 'Unspecified'
            and date_time >= current_date - 14
        group by 1,2
      )
      where content_rank = 1
    )
    //;
    ,




    //drop table workspace_al.page_name_syfy;
    //create table workspace_al.page_name_syfy as
    page_name_syfy as
    (
      -- Determine PageName
      select
        pageName
        , CONTENTID
      from
      (
        select 
          COALESCE(pagename, articletitle) as pageName
          , CONTENTID
          , count(distinct VISITS_CALC_ID) as visit_name
          , RANK() OVER(PARTITION BY CONTENTID ORDER BY visit_name DESC) content_rank
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_SYFY_COM_BLOG"
        where Page_Views != 0
            and pageurl is not null
            and custom_sub_section != 'Unspecified'
            and date_time >= current_date - 14
        group by 1,2
      )
      where content_rank = 1
    )
    //;
    ,



    //drop table workspace_al.page_name_oxygen;
    //create table workspace_al.page_name_oxygen as
    page_name_oxygen as
    (
      -- Determine PageName
      select
        pageName
        , CONTENTID
      from
      (
        select 
          COALESCE(pagename, articletitle) as pageName
          , CONTENTID
          , count(distinct VISITS_CALC_ID) as visit_name
          , RANK() OVER(PARTITION BY CONTENTID ORDER BY visit_name DESC) content_rank
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_OXYGEN_COM_BLOG"
        where Page_Views != 0
            and pageurl is not null
            and custom_sub_section != 'Unspecified'
            and date_time >= current_date - 14
        group by 1,2
      )
      where content_rank = 1
    )
    //;
    //,






    (
      select
        a.*
        , b.visit_count_entire_site
      from
      (
        ----------------------------------------------------------------------------
        -- NBC
        ----------------------------------------------------------------------------
        select
          'nbc' as brand
          , date(DATE_TRUNC('Day', date_time)) AS date_day
          , PUBLISHEDDATE
          , case when dayofweek(date(DATE_TRUNC('Day', date_time))) = 0 then date(DATE_TRUNC('Day', date_time)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', date_time)))) end as week
          , case when dayofweek(date(DATE_TRUNC('Day', PUBLISHEDDATE))) = 0 then date(DATE_TRUNC('Day', PUBLISHEDDATE)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', PUBLISHEDDATE)))) end as published_week
          , date(DATE_TRUNC('MONTH', date_time)) AS date_month
          , DATE_TRUNC('MONTH', PUBLISHEDDATE) AS published_date_month
          , POST_VISID_HIGH || '-' || POST_VISID_LOW as visitor_id
          , content.pageName
          , case when content.pageName is null then 'Remove' else 'Keep' end as page_name_filter
          , VISITS_CALC_ID
          , user_server
          , case when referrer_type = 'Typed/Bookmarked' and visit_ref_domain is null then 'Direct'
        when referrer_type = 'Search Engines' then 'Search'
        when referrer_type = 'Social Networks' then 'Social'
        when referrer_type = 'Other Web Sites' then 'Other'
        else 'NA' end as referral_source 
          , main.CONTENTID as content_id
          , CONTENT_Type
          , case when Visit_Num = 1 then 'New' else 'Returning' end as new_or_returning_user
          , sum(Page_Views) as Page_Views
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_NBC_COM_BLOG" main
        left join page_name_nbc as content on content.CONTENTID = main.CONTENTID
        where Page_Views != 0
          and pageurl is not null
          and custom_sub_section != 'Unspecified'
          and main.CONTENTID != 'Unspecified'
          and date_day >= current_date - 100
          -- and date_day >= '2022-02-22'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
      ) a
      left join 
      ---------------------------------------------------------------------------
      -- Site Overall
      (
        select
          case when dayofweek(date(DATE_TRUNC('Day', date_time))) = 0 then date(DATE_TRUNC('Day', date_time)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', date_time)))) end as week
          , count(distinct VISITS_CALC_ID)  as visit_count_entire_site
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_NBC_COM_BLOG" where week >= current_date - 100
        group by 1
      ) b on a.week = b.week
      ---------------------------------------------------------------------------
    )

    UNION ALL

    (
      select
        a.*
        , b.visit_count_entire_site
      from
      (
        ----------------------------------------------------------------------------
        -- USA
        ----------------------------------------------------------------------------
        select
          'usa' as brand
          , date(DATE_TRUNC('Day', date_time)) AS date_day
          , PUBLISHEDDATE
          , case when dayofweek(date(DATE_TRUNC('Day', date_time))) = 0 then date(DATE_TRUNC('Day', date_time)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', date_time)))) end as week
          , case when dayofweek(date(DATE_TRUNC('Day', PUBLISHEDDATE))) = 0 then date(DATE_TRUNC('Day', PUBLISHEDDATE)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', PUBLISHEDDATE)))) end as published_week
          , date(DATE_TRUNC('MONTH', date_time)) AS date_month
          , DATE_TRUNC('MONTH', PUBLISHEDDATE) AS published_date_month
          , POST_VISID_HIGH || '-' || POST_VISID_LOW as visitor_id
          , content.pageName
          , case when content.pageName is null then 'Remove' else 'Keep' end as page_name_filter
          , VISITS_CALC_ID
          , user_server
          , case when referrer_type = 'Typed/Bookmarked' and visit_ref_domain is null then 'Direct'
        when referrer_type = 'Search Engines' then 'Search'
        when referrer_type = 'Social Networks' then 'Social'
        when referrer_type = 'Other Web Sites' then 'Other'
        else 'NA' end as referral_source 
          , main.CONTENTID as content_id
          , CONTENT_Type
          , case when Visit_Num = 1 then 'New' else 'Returning' end as new_or_returning_user
          , sum(Page_Views) as Page_Views
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_USA_NETWORK_COM_BLOG" main
        left join page_name_usa as content on content.CONTENTID = main.CONTENTID
        where Page_Views != 0
          and pageurl is not null
          and custom_sub_section != 'Unspecified'
          and main.CONTENTID != 'Unspecified'
          and date_day >= current_date - 100
          -- and date_day >= '2022-03-24'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
      ) a
      left join 
      ---------------------------------------------------------------------------
      -- Site Overall
      (
        select
          case when dayofweek(date(DATE_TRUNC('Day', date_time))) = 0 then date(DATE_TRUNC('Day', date_time)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', date_time)))) end as week
          , count(distinct VISITS_CALC_ID)  as visit_count_entire_site
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_USA_NETWORK_COM_BLOG" where week >= current_date - 100
        group by 1
      ) b on a.week = b.week
      ---------------------------------------------------------------------------
    )

    UNION ALL

    (
      select
        a.*
        , b.visit_count_entire_site
      from
      (
        ----------------------------------------------------------------------------
        -- Bravo
        ----------------------------------------------------------------------------
        select
          'bravo' as brand
          , date(DATE_TRUNC('Day', date_time)) AS date_day
          , PUBLISHEDDATE
          , case when dayofweek(date(DATE_TRUNC('Day', date_time))) = 0 then date(DATE_TRUNC('Day', date_time)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', date_time)))) end as week
          , case when dayofweek(date(DATE_TRUNC('Day', PUBLISHEDDATE))) = 0 then date(DATE_TRUNC('Day', PUBLISHEDDATE)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', PUBLISHEDDATE)))) end as published_week
          , date(DATE_TRUNC('MONTH', date_time)) AS date_month
          , DATE_TRUNC('MONTH', PUBLISHEDDATE) AS published_date_month
          , POST_VISID_HIGH || '-' || POST_VISID_LOW as visitor_id
          , content.pageName
          , case when content.pageName is null then 'Remove' else 'Keep' end as page_name_filter
          , VISITS_CALC_ID
          , user_server
          , case when referrer_type = 'Typed/Bookmarked' and visit_ref_domain is null then 'Direct'
        when referrer_type = 'Search Engines' then 'Search'
        when referrer_type = 'Social Networks' then 'Social'
        when referrer_type = 'Other Web Sites' then 'Other'
        else 'NA' end as referral_source 
          , main.CONTENTID as content_id
          , CONTENT_Type
          , case when Visit_Num = 1 then 'New' else 'Returning' end as new_or_returning_user
          , sum(Page_Views) as Page_Views
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_BRAVO_TV_BLOG" main
        left join page_name_bravo as content on content.CONTENTID = main.CONTENTID
        where Page_Views != 0
          and pageurl is not null
          and custom_sub_section != 'Unspecified'
          and main.CONTENTID != 'Unspecified'
          and date_day >= current_date - 100
          -- and date_day >= '2022-01-01'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
      ) a
      left join 
      ---------------------------------------------------------------------------
      -- Site Overall
      (
        select
          case when dayofweek(date(DATE_TRUNC('Day', date_time))) = 0 then date(DATE_TRUNC('Day', date_time)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', date_time)))) end as week
          , count(distinct VISITS_CALC_ID)  as visit_count_entire_site
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_BRAVO_TV_BLOG" where week >= current_date - 100
        group by 1
      ) b on a.week = b.week
      ---------------------------------------------------------------------------
    )

    UNION ALL

    (
      select
        a.*
        , b.visit_count_entire_site
      from
      (
        ----------------------------------------------------------------------------
        -- Oxygen
        ----------------------------------------------------------------------------
        select
          'oxygen' as brand
          , date(DATE_TRUNC('Day', date_time)) AS date_day
          , PUBLISHEDDATE
          , case when dayofweek(date(DATE_TRUNC('Day', date_time))) = 0 then date(DATE_TRUNC('Day', date_time)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', date_time)))) end as week
          , case when dayofweek(date(DATE_TRUNC('Day', PUBLISHEDDATE))) = 0 then date(DATE_TRUNC('Day', PUBLISHEDDATE)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', PUBLISHEDDATE)))) end as published_week
          , date(DATE_TRUNC('MONTH', date_time)) AS date_month
          , DATE_TRUNC('MONTH', PUBLISHEDDATE) AS published_date_month
          , POST_VISID_HIGH || '-' || POST_VISID_LOW as visitor_id
          , content.pageName
          , case when content.pageName is null then 'Remove' else 'Keep' end as page_name_filter
          , VISITS_CALC_ID
          , user_server
          , case when referrer_type = 'Typed/Bookmarked' and visit_ref_domain is null then 'Direct'
        when referrer_type = 'Search Engines' then 'Search'
        when referrer_type = 'Social Networks' then 'Social'
        when referrer_type = 'Other Web Sites' then 'Other'
        else 'NA' end as referral_source 
          , main.CONTENTID as content_id
          , CONTENT_Type
          , case when Visit_Num = 1 then 'New' else 'Returning' end as new_or_returning_user
          , sum(Page_Views) as Page_Views
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_OXYGEN_COM_BLOG" main
        left join page_name_oxygen as content on content.CONTENTID = main.CONTENTID
        where Page_Views != 0
          and pageurl is not null
          and custom_sub_section != 'Unspecified'
          and main.CONTENTID != 'Unspecified'
          and date_day >= current_date - 100
          -- and date_day >= '2022-01-01'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
      ) a
      left join 
      ---------------------------------------------------------------------------
      -- Site Overall
      (
        select
          case when dayofweek(date(DATE_TRUNC('Day', date_time))) = 0 then date(DATE_TRUNC('Day', date_time)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', date_time)))) end as week
          , count(distinct VISITS_CALC_ID)  as visit_count_entire_site
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_OXYGEN_COM_BLOG" where week >= current_date - 100
        group by 1
      ) b on a.week = b.week
      ---------------------------------------------------------------------------
    )

    UNION ALL

    (
      select
        a.*
        , b.visit_count_entire_site
      from
      (
        ----------------------------------------------------------------------------
        -- SYFY
        ----------------------------------------------------------------------------
        select
          'syfy' as brand
          , date(DATE_TRUNC('Day', date_time)) AS date_day
          , PUBLISHEDDATE
          , case when dayofweek(date(DATE_TRUNC('Day', date_time))) = 0 then date(DATE_TRUNC('Day', date_time)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', date_time)))) end as week
          , case when dayofweek(date(DATE_TRUNC('Day', PUBLISHEDDATE))) = 0 then date(DATE_TRUNC('Day', PUBLISHEDDATE)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', PUBLISHEDDATE)))) end as published_week
          , date(DATE_TRUNC('MONTH', date_time)) AS date_month
          , DATE_TRUNC('MONTH', PUBLISHEDDATE) AS published_date_month
          , POST_VISID_HIGH || '-' || POST_VISID_LOW as visitor_id
          , content.pageName
          , case when content.pageName is null then 'Remove' else 'Keep' end as page_name_filter
          , VISITS_CALC_ID
          , user_server
          , case when referrer_type = 'Typed/Bookmarked' and visit_ref_domain is null then 'Direct'
        when referrer_type = 'Search Engines' then 'Search'
        when referrer_type = 'Social Networks' then 'Social'
        when referrer_type = 'Other Web Sites' then 'Other'
        else 'NA' end as referral_source 
          , main.CONTENTID as content_id
          , CONTENT_Type
          , case when Visit_Num = 1 then 'New' else 'Returning' end as new_or_returning_user
          , sum(Page_Views) as Page_Views
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_SYFY_COM_BLOG" main
        left join page_name_syfy as content on content.CONTENTID = main.CONTENTID
        where Page_Views != 0
          and pageurl is not null
          and custom_sub_section != 'Unspecified'
          and main.CONTENTID != 'Unspecified'
          -- and date_day >= current_date - 100
          and date_day >= '2022-01-01'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
      ) a
      left join 
      ---------------------------------------------------------------------------
      -- Site Overall
      (
        select
          case when dayofweek(date(DATE_TRUNC('Day', date_time))) = 0 then date(DATE_TRUNC('Day', date_time)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', date_time)))) end as week
          , count(distinct VISITS_CALC_ID)  as visit_count_entire_site
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_SYFY_COM_BLOG" where week >= current_date - 100
        group by 1
      ) b on a.week = b.week
      ---------------------------------------------------------------------------
    )




    """

    if service_account == 'no':
        df = run_sql(engine, query_name, query)
    elif service_account == 'yes':
        df = service_account_sql(query, column_names)
    else:
        """Please select service_account = 'yes' or 'no'"""

    return df



# Rolling 7 Days

def run_rolling_visits_per_post_sql(engine, column_names, service_account):
    query_name = 'Rolling'

    query = """

    (
      ----------------------------------------------------------------------------
      --NBC
      ----------------------------------------------------------------------------
      select
        brand
        , PUBLISHEDDATE
        , published_week
        , visit_date
        , contentid
        , referral_platform
        , count(distinct VISITS_CALC_ID) as VISIT_NUM
      from 
      (
        select
          'nbc' as brand
          , case when PUBLISHEDDATE < '2022-02-22' then '2022-02-22' else PUBLISHEDDATE end as PUBLISHEDDATE
          , case when dayofweek(PUBLISHEDDATE) = 0 then PUBLISHEDDATE else DATEADD(Day ,-1, DATE_TRUNC('Week', PUBLISHEDDATE)) end as published_week
          , date(date_time) as visit_date
          , CONTENTID 
          , VISITS_CALC_ID
          , case when referrer_type = 'Typed/Bookmarked' and visit_ref_domain is null then 'Direct'
        when referrer_type = 'Search Engines' then 'Search'
        when referrer_type = 'Social Networks' then 'Social'
        when referrer_type = 'Other Web Sites' then 'Other'
        else 'NA' end as referral_platform 
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_NBC_COM_BLOG"
        where date_time >= '2022-02-22'
          and PUBLISHEDDATE is not null
          and pageName is not null
          and lower(CONTENT_TYPE) in ('microsite', 'blog post', 'amp')
          and custom_sub_section != 'Unspecified'
      )
      group by 1,2,3,4,5,6
      order by 1,2,3,4,5,6
    )

    union all

    (
      ----------------------------------------------------------------------------
      --USA
      ----------------------------------------------------------------------------
      select
        brand
        , PUBLISHEDDATE
        , published_week
        , visit_date
        , contentid
        , referral_platform
        , count(distinct VISITS_CALC_ID) as VISIT_NUM
      from 
      (
        select
          'usa' as brand
          , case when PUBLISHEDDATE < '2022-03-24' then '2022-03-24' else PUBLISHEDDATE end as PUBLISHEDDATE
          , case when dayofweek(PUBLISHEDDATE) = 0 then PUBLISHEDDATE else DATEADD(Day ,-1, DATE_TRUNC('Week', PUBLISHEDDATE)) end as published_week
          , date(date_time) as visit_date
          , CONTENTID
          , VISITS_CALC_ID
          , case when referrer_type = 'Typed/Bookmarked' and visit_ref_domain is null then 'Direct'
        when referrer_type = 'Search Engines' then 'Search'
        when referrer_type = 'Social Networks' then 'Social'
        when referrer_type = 'Other Web Sites' then 'Other'
        else 'NA' end as referral_platform 
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_USA_NETWORK_COM_BLOG"
        where date_time >= '2022-03-24'
          and PUBLISHEDDATE is not null
          and pageName is not null
          and lower(CONTENT_TYPE) in ('microsite', 'blog post', 'amp')
          and custom_sub_section != 'Unspecified'
      )
      group by 1,2,3,4,5,6
      order by 1,2,3,4,5,6
    )

    union all

    (
      ----------------------------------------------------------------------------
      --Bravo
      ----------------------------------------------------------------------------
      select
        brand
        , PUBLISHEDDATE
        , published_week
        , visit_date
        , contentid
        , referral_platform
        , count(distinct VISITS_CALC_ID) as VISIT_NUM
      from 
      (
        select
          'bravo' as brand
          , PUBLISHEDDATE
          , case when dayofweek(PUBLISHEDDATE) = 0 then PUBLISHEDDATE else DATEADD(Day ,-1, DATE_TRUNC('Week', PUBLISHEDDATE)) end as published_week
          , date(date_time) as visit_date
          , CONTENTID
          , VISITS_CALC_ID
          , case when referrer_type = 'Typed/Bookmarked' and visit_ref_domain is null then 'Direct'
        when referrer_type = 'Search Engines' then 'Search'
        when referrer_type = 'Social Networks' then 'Social'
        when referrer_type = 'Other Web Sites' then 'Other'
        else 'NA' end as referral_platform 
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_BRAVO_TV_BLOG"
        where date_time >= '2022-01-01'
          and PUBLISHEDDATE is not null
          and pageName is not null
          and CONTENT_TYPE in ('Microsite', 'Blog Post', 'AMP')
          and custom_sub_section != 'Unspecified'
      )
      group by 1,2,3,4,5,6
      order by 1,2,3,4,5,6
    )

    union all

    (
      ----------------------------------------------------------------------------
      --OXYGEN
      ----------------------------------------------------------------------------
      select
        brand
        , PUBLISHEDDATE
        , published_week
        , visit_date
        , contentid
        , referral_platform
        , count(distinct VISITS_CALC_ID) as VISIT_NUM
      from 
      (
        select
          'oxygen' as brand
          , PUBLISHEDDATE
          , case when dayofweek(PUBLISHEDDATE) = 0 then PUBLISHEDDATE else DATEADD(Day ,-1, DATE_TRUNC('Week', PUBLISHEDDATE)) end as published_week
          , date(date_time) as visit_date
          , CONTENTID
          , VISITS_CALC_ID
          , case when referrer_type = 'Typed/Bookmarked' and visit_ref_domain is null then 'Direct'
        when referrer_type = 'Search Engines' then 'Search'
        when referrer_type = 'Social Networks' then 'Social'
        when referrer_type = 'Other Web Sites' then 'Other'
        else 'NA' end as referral_platform 
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_OXYGEN_COM_BLOG"
        where date_time >= '2022-01-01'
          and PUBLISHEDDATE is not null
          and pageName is not null
          and CONTENT_TYPE in ('Microsite', 'Blog Post', 'AMP')
          and custom_sub_section != 'Unspecified'
      )
      group by 1,2,3,4,5,6
      order by 1,2,3,4,5,6
    )

    union all

    (
      ----------------------------------------------------------------------------
      --SYFY
      ----------------------------------------------------------------------------
      select
        brand
        , PUBLISHEDDATE
        , published_week
        , visit_date
        , contentid
        , referral_platform
        , count(distinct VISITS_CALC_ID) as VISIT_NUM
      from 
      (
        select
          'syfy' as brand
          , PUBLISHEDDATE
          , case when dayofweek(PUBLISHEDDATE) = 0 then PUBLISHEDDATE else DATEADD(Day ,-1, DATE_TRUNC('Week', PUBLISHEDDATE)) end as published_week
          , date(date_time) as visit_date
          , CONTENTID
          , VISITS_CALC_ID
          , case when referrer_type = 'Typed/Bookmarked' and visit_ref_domain is null then 'Direct'
        when referrer_type = 'Search Engines' then 'Search'
        when referrer_type = 'Social Networks' then 'Social'
        when referrer_type = 'Other Web Sites' then 'Other'
        else 'NA' end as referral_platform 
        from "WALDO_PROD"."OMNITURE_BLOG"."OMNITURE_SYFY_COM_BLOG"
        where date_time >= '2022-01-01'
          and PUBLISHEDDATE is not null
          and pageName is not null
          and CONTENT_TYPE in ('Microsite', 'Blog Post', 'AMP')
          and custom_sub_section != 'Unspecified'
      )
      group by 1,2,3,4,5,6
      order by 1,2,3,4,5,6
    )
        """

    if service_account == 'no':
        df = run_sql(engine, query_name, query)
    elif service_account == 'yes':
        df = service_account_sql(query, column_names)
    else:
        """Please select service_account = 'yes' or 'no'"""

    return df




# Video FEP/SFV
def run_video_sql(engine, column_names, service_account):
    query_name = 'Video FEP/SFV'

    query = """
          select
      brand
      , date_day
      , week
      , date_month
      , video_type
      , show
      , visitor_id_calc
      , fep_starts
      , sfv_starts
    from
    (
      select
        case when lower(APPNAME) like '%bravo%' then 'bravo'
               when lower(APPNAME) like '%oxygen%' then 'oxygen'
               when lower(APPNAME) like '%usa%' then 'usa'
               when lower(APPNAME) like '%nbc%' then 'nbc'
               when lower(APPNAME) like '%syfy%' then 'syfy'
               else APPNAME end as brand
        , date(DATE_TRUNC('Day', REPORTDATE)) AS date_day
        , case when dayofweek(date(DATE_TRUNC('Day', REPORTDATE))) = 0 then date(DATE_TRUNC('Day', REPORTDATE)) else DATEADD(Day ,-1, DATE_TRUNC('Week', date(DATE_TRUNC('Day', REPORTDATE)))) end as week
        , date(DATE_TRUNC('MONTH', REPORTDATE)) AS date_month
        , case when lower(CONTENT_TYPE) like '%clip%' then 'SFV'
               when lower(CONTENT_TYPE) like '%episode%' then 'FEP'
               else 'Other' end as video_type
        , SHOW
        , VISITOR_ID_CALC
        , case when lower(video_page_url) like '%usa-insider%' then 'Editorial'
              when lower(video_page_url) like '%nbc-insider%' then 'Editorial'
              when lower(video_page_url) like '%the-daily-dish%' then 'Editorial'
              when lower(video_page_url) like '%style-living%' then 'Editorial'
              when lower(video_page_url) like '%syfy-wire%' then 'Editorial'
              when lower(video_page_url) like '%crime-news%' then 'Editorial'
              when lower(video_page_url) like '%true-crime-buzz%' then 'Editorial'
              else 'NA'
              end as page_type
        , sum(FEP_VODEPISODESTART) as fep_starts
        , sum(FEP_VODCLIPSTART) as sfv_starts
      from "WALDO_PROD"."OMNITURE_TVE"."UNIFIED_OMNITURE_FEP_DAILY"
      where appname is not null
        and brand in ('bravo', 'usa', 'nbc', 'oxygen', 'syfy')
        and lower(content_type) != 'unspecified'
        and REPORTDATE >= current_date - 100
        and page_type != 'NA'
      group by 1,2,3,4,5,6,7,8
    )

    """
    
    if service_account == 'no':
        df = run_sql(engine, query_name, query)
    elif service_account == 'yes':
        df = service_account_sql(query, column_names)
    else:
        """Please select service_account = 'yes' or 'no'"""

    return df