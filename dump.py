import pymongo
import query
import sys
import os
import inspect
import json
import argparse
import datetime
import time
import re

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from zabbix import pyzabbix_sender

reload(sys)
sys.setdefaultencoding('utf-8')

args = {}


def get_parameters():
    ''' GETS THE COMMAND LINE PARAMETERS '''
    global args
    parser = argparse.ArgumentParser(description='Collects information about events from FB using the location and insert to a MongoDB.')
    parser.add_argument('-t','--type', help='Type of data: date_default, date_timestamp, date_milliseconds, \
                                             date_datetime, twitter_default or twitter_massive', required=True)
    parser.add_argument('-s','--server', help='Name of the MongoDB server', required=True)
    parser.add_argument('-p','--persistence', help='Name of the MongoDB persistence slave', required=False)
    parser.add_argument('-d','--database', help='Name of the MongoDB database', required=True)
    parser.add_argument('-c','--collection', help='Name of the DB collection', required=True)
    parser.add_argument('-f','--field', help='Name of the fields with the date (should be indexed)', required=False)
    parser.add_argument('-o','--outfile', help='Outfile name', required=True)
    parser.add_argument('-id','--initial_date', help='Initial date or datetime or timestamp', required=False)
    parser.add_argument('-fd','--final_date', help='Final date or datetime or timestamp. If null, just query for init day exact match', required=False)
    parser.add_argument('-zk','--zabbix_item_key', help='Zabbix trap', required=False)
    parser.add_argument('-zh','--zabbix_host', help='Zabbix host', required=False)
    parser.add_argument('-ci','--control_id', help='Comma separated list of IDs of targets collectors', required=False)
    args = vars(parser.parse_args())


def mongo_connection():
    ''' MONGO CONNECTION '''
    ERROR = True
    count_attempts = 0
    while ERROR:
        try:
            count_attempts += 1
            if (args['persistence'] is None):
                client = pymongo.MongoClient(args['server'])
            else: 
                client = pymongo.MongoClient([args['server'],args['persistence']])
            client.server_info()
            ERROR = False
        except pymongo.errors.ServerSelectionTimeoutError:
            time.sleep(3)
    db = client[args['database']]
    return db[args['collection']]



def output_write(data):
    ''' OUTPUT WRITE '''
    global args
    count = 0
    try:
        outfile = open(args['outfile'], 'w')
    except:
        print "Could not open the outfile"
        sys.exit()
    for record in data:
        count += 1
        del record['_id']
        for field in record:
            record[field] = str(record[field])
        print >> outfile,json.dumps(record)
    return count


def inform_zabbix(number):
    ''' INFORM ZABBIX MONITOR '''
    global args
    if args.has_key('zabbix_host') and args.has_key('zabbix_item_key'):
        pyzabbix_sender.send(args['zabbix_host'], args['zabbix_item_key'], number, pyzabbix_sender.get_zabbix_server())
    

def perform_query(collection):
    ''' READ THE DATA TYPE AND CALL THE METHOD '''
    print args['type']
    if (args['type'] == 'date_datetime'): return query.date_datetime(collection,args)
    elif (args['type'] == 'date_default'): return query.date_default(collection,args)
    elif (args['type'] == 'date_milliseconds'): return query.date_milliseconds(collection,args)
    elif (args['type'] == 'date_timestamp'): return query.date_timestamp(collection,args)
    elif (args['type'] == 'last_execution'): return query.last_execution(collection,args)
    elif (args['type'] == 'twitter_default'): return query.twitter_default(collection,args)
    elif (args['type'] == 'twitter_massive'): return query.twitter_massive(collection,args)
    else: 
        print "Invalid type."
        sys.exit()


if __name__ == '__main__':
    try:
        get_parameters()
        collection = mongo_connection()
        data = perform_query(collection)
        print "AQUI 2"
        size = output_write(data)
        print size
        inform_zabbix(size)
    except:
        inform_zabbix(0)
