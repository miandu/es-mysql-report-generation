#!/usr/bin/python3
import json,datetime,time,argparse,logging,sys,os,re,copy,random
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
from boto3.dynamodb.conditions import Attr
import general_storage,sqs,utils,query,general_storage_mysql
from normalizer import Normalizer
from country_region import country_full_name
from country_region import country_region
from progress.bar import Bar
from pprint import pprint

## Global variables
comments_count_ht={}

class Normalizer_report_es_mysql(Normalizer):
    ## Normalizer class for comments from es (from mysql) to aggregated post-ads day items in mysql report table
    name="ads"
    target_source_rule={#'id':'object_id',
                        'object_id':'object_id',
                        'platform':lambda x: get_platform(x.source),
                        'adaccount_name':lambda x: get_adaccount_name(x.source),
                        'adaccount_id':lambda x: get_adaccount_id(x.source),
                        'campaign_name':lambda x: get_campaign_name(x.source),
                        'campaign_id':lambda x: get_campaign_id(x.source),
                        'adset_name':lambda x: get_adset_name(x.source),
                        'adset_id':lambda x: get_adset_id(x.source),
                        'ad_id':lambda x: get_ad_id(x.source),
                        'ad_link':lambda x: get_ad_link(x.source),
                        'message':'message',
                        'post_id':'post_id',
                        'date':lambda x:get_day(x.source),
                        'country_code':lambda x:get_country(x.source),
                        'topics':lambda x:get_topics(x.source),
                        'favorite_topics':'favorite_topics',
                        'favorite_sentiment':'favorite_sentiment',
                        #'tag':lambda x:get_tags(x.source),
                        'sentiment':'sentiment',
                        'sentiment_positive':lambda x:get_sentiment_positive(x.source),
                        'sentiment_negative':lambda x:get_sentiment_negative(x.source),
                        'sentiment_total':lambda x:get_sentiment_total(x.source),
    }

class Normalizer_report_organic_es_mysql(Normalizer):
    ## Normalizer class for comments from es (from mysql) to aggregated post-ads day items in mysql report table
    name="organic"
    target_source_rule={#'id':'object_id',
                        'platform':lambda x: get_platform(x.source),
                        'post_link':lambda x: get_ad_link(x.source),
                        'post_id':'post_id',
                        'account_id':'page_id',
                        'account_name':"MAC",
                        'message':'message',
                        'date':lambda x:get_day(x.source),
                        'topics':lambda x:get_topics(x.source),
                        'tag':lambda x:get_tags(x.source),
                        'country_code':lambda x:get_country(x.source,True),
                        'sentiment':'sentiment',
                        'sentiment_positive':lambda x:get_sentiment_positive(x.source),
                        'sentiment_negative':lambda x:get_sentiment_negative(x.source),
                        'sentiment_total':lambda x:get_sentiment_total(x.source),
    }

def get_platform(x):
    platform_id={"1":'Facebook',
                 "6":'Facebook',
                 "11":'Facebook',
                 "2":'Instagram',
                 "12":'Instagram',
                 "15":'Instagram',
                 "3":'YouTube'
    }
    return platform_id.get(str(x['platform']),"Unknown")

def get_adaccount_name(x):
    return list(set([ad.get('adaccount_name','') for ad in x.get('ads',[])]))

def get_adaccount_id(x):
    return list(set([ad.get('adaccount_id','') for ad in x.get('ads',[])]))

def get_campaign_name(x):
    return list(set([ad.get('campaign_name','') for ad in x.get('ads',[])]))

def get_campaign_id(x):
    return list(set([ad.get('campaign_id','') for ad in x.get('ads',[])]))

def get_adset_name(x):
    return [ad.get('adset_name','') for ad in x.get('ads',[])]

def get_adset_id(x):
    return [ad.get('adset_id','') for ad in x.get('ads',[])]

def get_ad_id(x):
    return [ad.get('ad_id','') for ad in x.get('ads',[])]

def get_ad_link(x):
    return x['post'].get("link","")
    
def get_active(x):
    return [ad.get('active','') for ad in x.get('categs',[])]

def get_day(x):
    return datetime.datetime.fromtimestamp(int(x['unix_created_time'])).strftime('%m/%d/%Y')

def get_day_start_unix(x):
    int(time.mktime(datetime.datetime.fromtimestamp(int(x['unix_created_time'])).replace(hour=0, minute=0, second=0, microsecond=0).timetuple()))

def get_day_end_unix(x):
    int(time.mktime(datetime.datetime.fromtimestamp(int(x['unix_created_time'])).replace(hour=23, minute=59, second=59, microsecond=59).timetuple()))

def get_country(x,random=False):
    if random:
        return random.choice(list(country_region.keys()))
    else:
        return list(set(utils.flatten([ad.get('location',[]) for ad in x.get('ads',[])])))

def get_country_name(x):
    return [country_full_name.get(c) for c in x]

def get_region(x):
    return [country_region.get(c) for c in x]
    
def fix_topic(topic):
    return re.sub( '\s+', '_', re.sub('[^a-z_0-9 ]','',topic.lower())).strip()[:50]

def get_topics(x):
    topics=[]
    if favorite:
        topic_key='favorite_topics'
    else:
        topic_key='topics'
    for topic in x.get(topic_key):
        topics.append(fix_topic(topic))
    if len(topics)==0:
        topics=['no_topic']
    return topics


def get_tags(x):
    topics=get_topics(x)
    " | ".join(topics)

def get_sentiment_value(x,sentiment=None,topic=None):
    topics=[fix_topic(topic) for topic in x.get("topics",[])]
    ## if positive or negative
    if sentiment: 
        sentiments=x.get("sentiment",[])
        if sentiment in sentiments:
            #if sentiment with topic
            if topic:
                if topic in topics:
                     return 1
                else:
                    return 0
            ## if sentiment only
            else:
                return 1
        else:
            return 0
    ## if total
    else:
        if topic:
            if topic in topics:
                return 1
            else:
                return 0
        else:
            return 1
    
def get_sentiment_positive(x,topic=None):
    return get_sentiment_value(x,"positive",topic)

def get_sentiment_negative(x,topic=None):
    return get_sentiment_value(x,"negative",topic)

def get_sentiment_total(x,topic=None):
    return get_sentiment_value(x,None,topic)

def multiply_group_by_single_attr(items,group_by_attr):
    new_items=[]
    for item in items:
        for value in item[group_by_attr]:
                this_item=copy.deepcopy(item) 
                this_item[group_by_attr]=value
                new_items.append(this_item)
    return new_items

def value_list_to_string(items):
    aggregate_attr={}
    for item in items:
        for attr,val in item.items():
            aggregate_attr[attr]=1
            if isinstance(val, list):
                item[attr]="|".join(val)
    return items, aggregate_attr

def store_items_to_mysql(cf,table,items):
    items, aggregate_attr=value_list_to_string(items)
    store_report_table(cf,table,items,aggregate_attr)

def aggregate_by_values(cf,nl,items,seen_topics):
    ## group objects into new objects by attributes
    
    ## compute post_process attributes
    for item in items:
        item['country_name']=list(set(get_country_name(item.get("country_code",""))))
    for item in items:
        item['region']=list(set(get_region(item.get("country_code",""))))

    ## multiply record by country
    country_items=multiply_group_by_single_attr(items,'country_name')
    
    ## multiply record by region
    region_items=multiply_group_by_single_attr(items,'region')

    store_items_to_mysql(cf,cf.mysql_table_name_comments,items)
    store_items_to_mysql(cf,cf.mysql_table_name_country,country_items)
    store_items_to_mysql(cf,cf.mysql_table_name_region,region_items)

def normalize_es_item_into_mysql(cf,nl,i):
    
    nl.normalize_source_to_target(cf,i)
    return nl.target
    
def accumulate_count(attr,count):
    global comments_count_ht
    if attr in comments_count_ht:
        comments_count_ht[attr]=comments_count_ht[attr]+count
    else:
        comments_count_ht[attr]=count

def store_report_count_hash(cf,items):
    for i in items:
        for attr,val in i.items():
            if re.search("negative|positive|total",attr):
                accumulate_count(attr,val)
    accumulate_count("comments_total",len(items))        

def store_report_table(cf,table,items,attr_list):
    ## create table if not exists
    create_table_if_non_exists(cf,table)
    ## add columns if not exists
    add_columns_if_non_exists(cf,table,attr_list)    
    ## Insert into mysql
    general_storage_mysql.insert_to_mysql_list(cf,table,items)    

def aggregate_es_item_into_mysql(cf,nl,db_items):
    ## Main function to call normalizer to normalize object from es report object to mysql report object, 
    ## grouping objects 
    ## then insert aggreated objects to mysql database
    es_targets=[]
    seen_topics=[]
    for i in db_items:
        es_targets.append(normalize_es_item_into_mysql(cf,nl,i))
    for item in es_targets:
        for t in item.get("topics",[]):
            if not t in seen_topics:
                seen_topics.append(t)
    
        for k in seen_topics:
            item[k+"_positive"]=get_sentiment_positive(item,k)
            item[k+"_negative"]=get_sentiment_negative(item,k)
            item[k+"_total"]=get_sentiment_total(item,k)
        
    ## accumulate count for report stats 
    store_report_count_hash(cf,es_targets)

    ## output comments object into three tables, comments, country, region
    aggregate_by_values(cf,nl,es_targets,seen_topics)


def get_all_columns(cf,table):
    connection = general_storage_mysql.create_connection(cf)
    return[x['Field'] for x in general_storage_mysql.execute_query(connection,"SHOW COLUMNS FROM %s" %(table))]    

def add_columns_if_non_exists(cf,table,item):
    attributes,values = general_storage_mysql.simple_json_to_mysql_query(item)
    non_seen_attributes=[]
    db_columns=get_all_columns(cf,table)
    for attr in attributes.split(","):
        if not attr in db_columns:
            non_seen_attributes.append(attr)
    if len(non_seen_attributes)>0:
        columns=make_columns_from_attributes(non_seen_attributes)    
        add_column_to_mysql(cf,table,columns,non_seen_attributes)

def add_column_to_mysql(cf,table,columns,keys):
    connection = general_storage_mysql.create_connection(cf)
    ##query_str ="alter table %s add column %s,add key %s" %(table,
    ##                                                       ", add column ".join(columns),
    ##                                                       ", add key ".join(["%s (%s)" %(x,x) for x in keys]))
    query_str ="alter table %s add column %s" %(table,
                                                ", add column ".join(columns))
    general_storage_mysql.execute_query(connection,query_str)

def create_table_if_non_exists(cf,table):
    connection = general_storage_mysql.create_connection(cf)
    query_str = """CREATE TABLE IF NOT EXISTS """+table+ " ( `id` int(11) NOT NULL AUTO_INCREMENT,PRIMARY KEY (`id`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    general_storage_mysql.execute_query(connection,query_str)

def make_columns_from_attributes(attributes):
    columns=[]
    for attr in attributes:
        if re.search("negative|positive|total",attr):
            columns.append("`%s` int(10) default 0" %(attr))
        else:
            columns.append("`%s` varchar(255)" %(attr))
    return columns

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Normalizer for twitter between DynamoDB and mysql') 
    parser.add_argument('config', type=str, help='an config file for normalizer')
    parser.add_argument('--query', type=str, default="_exists_:post_id", help='query to get data for normalizer using lucene format, e.g."_exists_:post_id"')
    parser.add_argument('--type', type=str, default="ads", help='type of the comment, for ads or organic')
    parser.add_argument('--favorite', type=bool, default=False, help='export favorite comment only or all')
    parser.add_argument('--date_start', type=str, default=None, help='start day of the query')
    parser.add_argument('--date_end', type=str, default=None, help='end day of the query')
    args = parser.parse_args()
    config = __import__(args.config)
    cf =config.Config()
    if args.type=="ads":
        nl = Normalizer_report_es_mysql()    
    else:
        nl = Normalizer_report_organic_es_mysql()
   
    favorite=args.favorite    
    if favorite:
        print("Exorting favorite comments")

    if args.date_start:
        date_start_unix=utils.get_date_unix(utils.parse_time(args.date_start))
        current_start_unix=date_start_unix
        #Process data: _exists_:post_id AND unix_created_time:>=1525132800 AND unix_created_time:<1525219200
        #current_start_unix=1525132800
        if args.date_end:
            date_end_unix=utils.get_date_unix(utils.parse_time(args.date_end))
        else:
            date_end_unix=utils.get_current_posix_number()        
        while current_start_unix<=date_end_unix:
            query_str=args.query+" AND unix_created_time:>=%s AND unix_created_time:<%s" %(current_start_unix,current_start_unix+86400)
            if favorite:
                query_str=query_str+" AND categs.active:3"
            current_start_unix=current_start_unix+86400
            print("Process data: "+query_str)
            total,posts = query.query_items(cf,query_str)
            if len(posts)>0:
                aggregate_es_item_into_mysql(cf,nl,posts)
        store_report_table(cf,cf.mysql_table_name_count,[comments_count_ht],comments_count_ht)
    else:
        if favorite:
            query_str=args.query+" AND categs.active:3"
        print("Process data: "+query_str)
        total,posts = query.query_items(cf,query_str)
        aggregate_es_item_into_mysql(cf,nl,posts)
        
        store_report_table(cf,cf.mysql_table_name_count,[comments_count_ht],comments_count_ht)
    
