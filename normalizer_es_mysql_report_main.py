#!/usr/bin/python3
import json,datetime,time,argparse,logging,sys,os,re,copy,random
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
from boto3.dynamodb.conditions import Attr
import general_storage,sqs,utils,query,general_storage_mysql
from normalizer import Normalizer
from country_region import country_region
from progress.bar import Bar
from pprint import pprint

## Global variables
comments_count_ht={}

class Normalizer_report_es_mysql(Normalizer):
    ## Normalizer class for comments from es (from mysql) to aggregated post-ads day items in mysql report table
    name="ads"
    target_source_rule={#'id':'object_id',
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
                        #'region':lambda x:get_region(x.source),
                        'country':lambda x:get_location(x.source),
                        'topics':lambda x:get_topics(x.source),
                        'tag':lambda x:get_tags(x.source),
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
                        #'region':lambda x:get_region(x.source),
                        'topics':lambda x:get_topics(x.source),
                        'tag':lambda x:get_tags(x.source),
                        'country':lambda x:get_random_location(),
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
    return [ad.get('adaccount_name','') for ad in x.get('ads',[])]

def get_adaccount_id(x):
    return [ad.get('adaccount_id','') for ad in x.get('ads',[])]

def get_campaign_name(x):
    return [ad.get('campaign_name','') for ad in x.get('ads',[])]

def get_campaign_id(x):
    return [ad.get('campaign_id','') for ad in x.get('ads',[])]

def get_adset_name(x):
    return [ad.get('adset_name','') for ad in x.get('ads',[])]

def get_adset_id(x):
    return [ad.get('adset_id','') for ad in x.get('ads',[])]

def get_ad_id(x):
    return [ad.get('ad_id','') for ad in x.get('ads',[])]

def get_ad_link(x):
    return x['post'].get("link","")
    
def get_day(x):
    return datetime.datetime.fromtimestamp(int(x['unix_created_time'])).strftime('%m/%d/%Y')

def get_day_start_unix(x):
    int(time.mktime(datetime.datetime.fromtimestamp(int(x['unix_created_time'])).replace(hour=0, minute=0, second=0, microsecond=0).timetuple()))

def get_day_end_unix(x):
    int(time.mktime(datetime.datetime.fromtimestamp(int(x['unix_created_time'])).replace(hour=23, minute=59, second=59, microsecond=59).timetuple()))

def get_location(x):
    return list(set(utils.flatten([ad.get('location',[]) for ad in x.get('ads',[])])))

def get_random_location():
    return random.choice(list(country_region.keys()))

def get_region(cf,x):
    return cf.country_region.get(x.get('country',""),"")
    
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

def multiply_group_by_attributes(items,seen_topics):
    new_items=[]
    for item in items:
        for i,ad_id in enumerate(item['ad_id']):
            for country in item['country']:
                this_item=copy.deepcopy(item) 
                this_item['adaccount_name']=item['adaccount_name'][i]
                this_item['adaccount_id']=item['adaccount_id'][i]
                this_item['campaign_name']=item['campaign_name'][i]
                this_item['campaign_id']=item['campaign_id'][i]
                this_item['adset_name']=item['adset_name'][i]
                this_item['adset_id']=item['adset_id'][i]
                this_item['ad_id']=ad_id
                this_item['country']=country
                this_item['region']=country
                ##for k in seen_topics:
                ##    this_item[k+"_positive"]=item.get(k+"_positive",0)
                ##    this_item[k+"_negative"]=item.get(k+"_negative",0)
                ##    this_item[k+"_total"]=item.get(k+"_total",0)

                new_items.append(this_item)
    return new_items

def aggregate_by_values(cf,nl,items,seen_topics):
    ## group objects into new objects by attributes
    group_by_attributes=cf.group_by_attributes
    copy_attributes=cf.copy_attributes
    group_items={}
    aggregate_items=[]
    ## multiply items with multiple values of ad_id and location
    if nl.name=="ads":
        items=multiply_group_by_attributes(items,seen_topics)
    
    print("multiply"+str(len(items)))
    print(items[0])

    store_report_table(cf,cf.mysql_table_name_comments,items)


    for item in items:
        key="_".join([str(item.get(x)) for x in group_by_attributes])
        if key in group_items:
            group_items[key].append(item)
        else:
            group_items[key]=[item]
            
    for key,group_items in group_items.items():
        mysql_item={}
        for k in group_by_attributes:
            mysql_item[k] = group_items[0][k]
        for k in copy_attributes:
            mysql_item[k] = group_items[0][k]
        ## compute sentiment per topic
        mysql_item['region']=get_region(cf,mysql_item)
        for k in seen_topics:
            positive=0
            negative=0
            total=0
            for item in group_items:
                positive+=item.get(k+"_positive",0)
                negative+=item.get(k+"_negative",0)
                total+=item.get(k+"_total",0)
            mysql_item[k+"_positive"]=positive
            mysql_item[k+"_negative"]=negative
            mysql_item[k+"_total"]=total

        ## compute overal sentiment
        positive=0
        negative=0
        total=0
        for i in group_items:
            positive+=i['sentiment_positive']
            negative+=i['sentiment_negative']
            total+=i['sentiment_total']
        mysql_item["sentiment_positive"]=positive
        mysql_item["sentiment_negative"]=negative
        mysql_item["sentiment_total"]=total
        aggregate_items.append(mysql_item)
    return aggregate_items

##def aggregate_by_values(group_by_attributes,copy_attributes,items):
##    ## group objects into new objects by attributes
##    group_items={}
##    aggregate_items=[]
##    seen_topics=[]
##    ## multiply items with multiple values of ad_id and location
##    items=multiply_group_by_attributes(items)
##    print("multiply"+str(len(items)))
##    for item in items:
##        for t in item.get("topics",[]):
##            if not t in seen_topics:
##                seen_topics.append(t)
##        key="_".join([str(item.get(x)) for x in group_by_attributes])
##        if key in group_items:
##            group_items[key].append(item)
##        else:
##            group_items[key]=[item]
##
##
##    for key,items in group_items.items():
##        mysql_item={}
##        for k in group_by_attributes:
##            mysql_item[k] = items[0][k]
##        for k in copy_attributes:
##            mysql_item[k] = items[0][k]
##        ## compute sentiment per topic
##        for k in seen_topics:
##            positive=0
##            negative=0
##            total=0
##            for i in items:
##                positive+=get_sentiment_positive(i,k)
##                negative+=get_sentiment_negative(i,k)
##                total+=get_sentiment_total(i,k)
##            mysql_item[k+"_positive"]=positive
##            mysql_item[k+"_negative"]=negative
##            mysql_item[k+"_total"]=total
##
##        ## compute overal sentiment
##        positive=0
##        negative=0
##        total=0
##        for i in items:
##            positive+=get_sentiment_positive(i)
##            negative+=get_sentiment_negative(i)
##            total+=get_sentiment_total(i)
##        mysql_item["sentiment_positive"]=positive
##        mysql_item["sentiment_negative"]=negative
##        mysql_item["sentiment_total"]=total
##        aggregate_items.append(mysql_item)
##    return aggregate_items

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

def store_report_table(cf,table,items):
    ## create table if not exists
    create_table_if_non_exists(cf,table)
            
    ## Insert into mysql
    for i in items:     
        ## add columns if not exists
        add_columns_if_non_exists(cf,table,i)    
        print(i)
        insert_to_mysql(cf,table,i)    

def aggreate_es_item_into_mysql(cf,nl,db_items):
    ## Main function to call normalizer to normalize object from es report object to mysql report object, 
    ## grouping objects by ads-day-country,
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
        
    #### compute overal sentiment
    ##positive=0
    ##negative=0
    ##total=0
    ##for i in es_targets:
    ##    positive+=get_sentiment_positive(i)
    ##    negative+=get_sentiment_negative(i)
    ##    total+=get_sentiment_total(i)
    ##item["sentiment_positive"]=positive
    ##item["sentiment_negative"]=negative
    ##item["sentiment_total"]=total    

    ## accumulate count for report stats 
    store_report_count_hash(cf,es_targets)
    
    ## aggregate comments object into post-ads-country-day objects
    sql_items=aggregate_by_values(cf,nl,es_targets,seen_topics)
    print(len(sql_items))
    store_report_table(cf,cf.mysql_table_name_main,sql_items)

##def aggreate_es_item_into_mysql(cf,db_items):
##    ## Main function to call normalizer to normalize object from es report object to mysql report object, 
##    ## grouping objects by ads-day-country,
##    ## then insert aggreated objects to mysql database
##    es_targets=[]
##    seen_topics=[]
##
##    for i in db_items:
##        es_targets.append(normalize_es_item_into_mysql(cf,i))
##    print(len(db_items))
##    print(len(es_targets))
##    
##    for item in es_targets:
##        for t in item.get("topics",[]):
##            if not t in seen_topics:
##                seen_topics.append(t)
##    
##    for k in seen_topics:
##        positive=0
##        negative=0
##        total=0
##        for i in es_targets:
##            positive+=get_sentiment_positive(i,k)
##            negative+=get_sentiment_negative(i,k)
##            total+=get_sentiment_total(i,k)
##        item[k+"_positive"]=positive
##        item[k+"_negative"]=negative
##        item[k+"_total"]=total
##
##    ## compute overal sentiment
##    positive=0
##    negative=0
##    total=0
##    for i in es_targets:
##        positive+=get_sentiment_positive(i)
##        negative+=get_sentiment_negative(i)
##        total+=get_sentiment_total(i)
##    item["sentiment_positive"]=positive
##    item["sentiment_negative"]=negative
##    item["sentiment_total"]=total    
##
##    ## accumulate count for report stats 
##    store_report_count_hash(cf,es_targets)
##    
##    ## aggregate comments object into post-ads-country-day objects
##    #sql_items=aggregate_by_values(cf.group_by_attributes,cf.copy_attributes,es_targets)
##    #print(len(sql_items))
##    #store_report_table(cf,cf.mysql_table_name_main,sql_items)
##

def get_all_columns(cf,table):
    connection = general_storage_mysql.create_connection(cf)
    return[x['Field'] for x in general_storage_mysql.execute_query(connection,"SHOW COLUMNS FROM %s" %(table))]    

def add_columns_if_non_exists(cf,table,item):
    attributes,values = general_storage_mysql.simple_json_to_mysql_query(item)
    print(attributes)
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
    print(query_str)
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

def insert_to_mysql(cf,table,item):
    connection = general_storage_mysql.create_connection(cf)
    attributes,values = general_storage_mysql.simple_json_to_mysql_query(item)
    query_str="insert into %s(%s) values(%s)" %(table,attributes,values)
    general_storage_mysql.execute_query(connection,query_str)


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
        print(utils.parse_time(args.date_start))
        date_start_unix=utils.get_date_unix(utils.parse_time(args.date_start))
        current_start_unix=date_start_unix
        #Process data: _exists_:post_id AND unix_created_time:>=1525132800 AND unix_created_time:<1525219200
        #current_start_unix=1525132800
        if args.date_end:
            date_end_unix=utils.get_date_unix(utils.parse_time(args.date_end))
        else:
            date_end_unix=utils.get_current_posix_number()        
        while current_start_unix<=date_end_unix:
            print(date_end_unix-current_start_unix)
            query_str=args.query+" AND unix_created_time:>=%s AND unix_created_time:<%s" %(current_start_unix,current_start_unix+86400)
            if favorite:
                query_str=query_str+" AND categs.active:3"
            current_start_unix=current_start_unix+86400
            print("Process data: "+query_str)
            total,posts = query.query_items(cf,query_str)
            if len(posts)>0:
                aggreate_es_item_into_mysql(cf,nl,posts)
            ##try:
            ##    aggreate_es_item_into_mysql(cf,posts)
            ##except Exception as e:
            ##    print(e)
        store_report_table(cf,cf.mysql_table_name_count,[comments_count_ht])
    else:
        if favorite:
            query_str=args.query+" AND categs.active:3"
        print("Process data: "+query_str)
        total,posts = query.query_items(cf,query_str)
        print(len(posts))
        aggreate_es_item_into_mysql(cf,nl,posts)
        
        store_report_table(cf,cf.mysql_table_name_count,[comments_count_ht])
    
