#!/usr/bin/python3

import general_config
class Config(general_config.Config):
    ## Mysql table
    mysql_table_name_main='n_mvr_main_test2'
    mysql_table_name_count='n_mvr_count_test4'
    mysql_table_name_comments='n_mvr_comments4'
    mysql_table_name_country='n_mvr_country4'
    mysql_table_name_region='n_mvr_region4'
    
    ## ES
    es_host='https://search-report-e6zu3zmdub22rqx6lesu6due2a.eu-west-1.es.amazonaws.com'
    table_name='export_nmvr2'
    es_filter_path=None

    ## aggregation
    group_by_attributes=["platform","ad_id","post_id","date","country"]
    copy_attributes=["adaccount_name","adaccount_id","campaign_name","campaign_id","adset_id","ad_link"]
    
