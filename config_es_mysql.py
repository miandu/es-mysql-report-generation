#!/usr/bin/python3

import general_config
class Config(general_config.Config):
    ## Mysql table
    mysql_table_name='test_n_mvr'
    
    ## ES
    es_host='https://search-report-e6zu3zmdub22rqx6lesu6due2a.eu-west-1.es.amazonaws.com'
    table_name='test_n_mvr'
    es_filter_path=None

    ## aggregation
    group_by_attributes=["platform","ad_id","post_id","date","country"]
    copy_attributes=["adaccount_name","adaccount_id","campaign_name","campaign_id","adset_id"]
