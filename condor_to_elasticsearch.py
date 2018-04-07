#!/usr/bin/env python

from __future__ import print_function
import os
import glob
from optparse import OptionParser
import logging
from functools import partial

from getpass import getpass

parser = OptionParser('usage: %prog [options] history_files')
parser.add_option('-a','--address',help='elasticsearch address')
parser.add_option('-n','--indexname',default='job_history',
                  help='index name (default job_history)')
parser.add_option('--collectors', default=False, action='store_true',
                  help='Args are collector addresses, not files')
parser.add_option('--history', default=False, action='store_true',
                  help='Read history instead of from the queue')
parser.add_option('--dailyindex', default=False, action='store_true',
                  help='Index pattern daily')
parser.add_option('-u', '--user', default=None, help='ES Cluster Username')
parser.add_option('-p', '--password', action='store_true', help='password prompt')
(options, args) = parser.parse_args()
if not args:
    parser.error('no condor history files or collectors')

if options.password:
    password = getpass()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s : %(message)s')


from condor_utils import *

if options.dailyindex:
    options.indexname += '-'+now.strftime("%Y.%m.%d")

def es_generator(entries):
    for data in entries:
        add_classads(data)
        data['_index'] = options.indexname
        data['_type'] = 'job_ad'
        data['_id'] = data['GlobalJobId'].replace('#','-').replace('.','-')
        if not options.history:
            data['_id'] += data['@timestamp']
        yield data

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

prefix = 'http'
address = options.address
if '://' in address:
    prefix,address = address.split('://')

if options.user is not None:
    url = '{}://{}:{}@{}'.format(prefix,options.user, password, address)
else:
    url = '{}://{}'.format(prefix, address)
logging.info('connecting to ES at %s',url)
es = Elasticsearch(hosts=[url],
                   timeout=5000)
es_import = partial(bulk, es, max_retries=20, initial_backoff=2, max_backoff=3600)

if options.collectors:
    for coll_address in args:
        gen = es_generator(read_from_collector(coll_address, history=options.history))
        success, _ = es_import(gen)
else:
    for path in args:
        for filename in glob.iglob(path):
            gen = es_generator(read_from_file(filename))
            success, _ = es_import(gen)
            logging.info('finished processing %s', filename)
