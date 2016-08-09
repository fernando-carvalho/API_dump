from filters import *
import pymongo
import datetime
import time

target_fields = [
    "_id", "id", "text", "created_at", "geo", "place", "coordinates", "entities",
    "_tmp_", "control", "lang", "source", "user.id", "user.screen_name", "user.location", "user.profile_image_url",
    "user.profile_image_url_https", "user.friends_count", "user.followers_count", "user.description", "user.lang",
    "retweeted_status.id", "retweeted_status.text", "retweeted_status.created_at",
    "retweeted_status.user.id", "retweeted_status.user.screen_name",
    "retweeted_status.retweet_count", "retweeted_status.entities"
]


def convert_date_to_timestamp(date, delta):
    ''' CONVERT DATE TO TIMESTAMP'''
    human_date = datetime.datetime.strptime(date,'%Y-%m-%d') + datetime.timedelta(days=delta)
    return time.mktime(human_date.timetuple())


def date_milliseconds(collection, args):
    ''' QUERY DATA USING A DATE IN MILLISECONDS'''
    if args['final_date']: args['final_date'] = int(convert_date_to_timestamp(args['final_date'],0))
    else:args['final_date'] = int(convert_date_to_timestamp(args['initial_date'],1)) * 1000
    args['initial_date'] = int(convert_date_to_timestamp(args['initial_date'],0)) * 1000
    data = collection.find({args['field']: {'$gte': args['initial_date'], '$lt': args['final_date'] }}, no_cursor_timeout=False)
    return data


def date_timestamp(collection, args):
    ''' QUERY DATA USING A DATE IN TIMESTAMP'''
    if args['final_date']:args['final_date'] = int(convert_date_to_timestamp(args['final_date'], 0))
    else:args['final_date'] = int(convert_date_to_timestamp(args['initial_date'], 1))
    args['initial_date'] = int(convert_date_to_timestamp(args['initial_date'], 0))
    data = collection.find({args['field']: {'$gte': args['initial_date'], '$lt': args['final_date'] }}, no_cursor_timeout=False)
    return data


def date_datetime(collection, args):
    ''' QUERY DATA USING A DATE IN DATETIME FORMAT'''
    if args['final_date']:
        args['final_date'] = datetime.datetime.strptime(args['final_date'],'%Y-%m-%d')
    else:
        args['final_date'] = datetime.datetime.strptime(args['initial_date'],'%Y-%m-%d') + datetime.timedelta(days=1)
    args['initial_date'] = datetime.datetime.strptime(args['initial_date'],'%Y-%m-%d')
    data = collection.find({args['field']: {'$gte': args['initial_date'], '$lt': args['final_date'] }}, no_cursor_timeout=False)
    return data


def date_default(collection, args):
    ''' QUERY DATA USING A DATE IN FORMAT YYYY-MM-DD'''
    if args['final_date']:
        data = collection.find({args['field']: {'$gte': args['initial_date'], '$lt': args['final_date'] }}, no_cursor_timeout=False)
    else:
        data = collection.find({args['field']: {'$regex' : ".*"+args['initial_date']+".*"}}, no_cursor_timeout=False)
    return data


def last_execution(collection, args):
    ''' QUERY DATA USING THE LAST EXECUTION DATE'''
    last_date = collection.find().sort([['_id',pymongo.DESCENDING]]).limit(1)[0][args['field']]
    data = collection.find({args['field']: {'$regex' : ".*"+last_date+".*"}}, no_cursor_timeout=False)
    return data


def twitter_massive(collection, args):
    ''' PERFORMS AN HEURISTIC QUERY IN A HUGE TWITTER DATABASE'''
    # FORMAT THE DATES
    args['initial_date'] = datetime.datetime.strptime(args['initial_date'], '%Y-%m-%d-%H:%M:%S')
    args['final_date'] = datetime.datetime.strptime(args['final_date'], '%Y-%m-%d-%H:%M:%S')
    # GET ALL THE 1% MOST RECENT TWEETS OF THE DATABASE
    size = int(collection.find().count() * 0.01)
    records = collection.find().sort([['_id',pymongo.DESCENDING]]).limit(size)
    # FILTER THE TWEETS THAT SHOULD BE WRITTEN 
    started = False
    data = []
    for record in records:
        if record['created_at'] >= args['initial_date'] and record['created_at'] < args['final_date']:
            started = True
            record = dict_find(target_fields, record)
            record['human_date'] = str(record['created_at'])
            data.append(dict_find(target_fields, record))
        # IF HAS FINISHED PRITING THE TARGET DATA, STOP AND FINISH
        else:
            if started:
                return data
    return data
   

def twitter_default(collection,args):
    ''' PERFORMS A REGULAR QUERY IN A TWITTER DATABASE'''
    # FORMAT THE DATES
    args['initial_date'] = datetime.datetime.strptime(args['initial_date'], '%Y-%m-%d-%H:%M:%S')
    args['final_date'] = datetime.datetime.strptime(args['final_date'], '%Y-%m-%d-%H:%M:%S')
    records = collection.find({'created_at': {'$lt': args['final_date'], '$gte': args['initial_date']}})
    size = collection.find({'created_at': {'$lt': args['final_date'], '$gte': args['initial_date']}}).count()
    data = []
    # SELECT THE TWEETS USING THE CONTROL ID AND THE TWEETS FIELDS
    if args['control_id']:
        for record in records:
            target = False
            try:
                for collector in record['control']['coletas']:
                    if (str(collector['id']) in str(args['control_id'].split(','))):
                        target = True
            except KeyError:
                for collector in record['control']['coleta']:
                    if (str(collector) in str(args['control_id'].split(','))):
                        target = True
            if (target):
                record = dict_find(target_fields, record)
                record['human_date'] = str(record['created_at'])
                data.append(record)
    # SELECT THE TWEETS FIELDS 
    else:
        for record in records:
            record = dict_find(target_fields, record)
            record['human_date'] = str(record['created_at'])
            data.append(record)
    print "INICIO"
    return data


def whole_database(collection, args):
    ''' QUERY ALL DATA IN A DATABASE'''
    data = collection.find(no_cursor_timeout=False)
    return data
