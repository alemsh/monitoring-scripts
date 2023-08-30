#!/usr/bin/env python3

import os, sys
import glob
import json
from optparse import OptionParser
import logging
import htcondor, classad
from condor_utils import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s : %(message)s')

parser = OptionParser('usage: %prog [options] history_files')

parser.add_option('-o','--stdout',default=False, action='store_true',
                  help='dump json to stdout')
parser.add_option('-d','--daemon',default=False, action='store_true',
                  help='read history from')
parser.add_option('-f','--histfile',
                  help='history file to read from')
(options, args) = parser.parse_args()
if not args:
    parser.error('no condor history files or collectors')


def resolve_ads():

if options.histfile:
    for path in args:
        for filename in glob.iglob(path):
            ads = read_from_file(options.histfile)

if options.daemon:
    try:
        ads = read_from_collector(args, history=True)
    except htcondor.HTCondorIOError as e:
        failed = e
        logging.error(f'Condor error: {e}')

for ad in ads:
    for attribute in ad:
        if type(ad[attribute]) is classad.ExprTree:
            ads[attribute] = ad[attribute].eval(classad.ClassAd(ad))

for ad in ads: 
    json.dump(ad, sys.stdout)