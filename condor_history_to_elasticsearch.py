from __future__ import print_function
import os
import glob
import gzip
from optparse import OptionParser
from datetime import datetime,timedelta
import time
import logging

import classad

import requests

now = datetime.utcnow()
zero = datetime.utcfromtimestamp(0).isoformat()

good_keys = {
    'JobStatus':0.,
    'Cmd':'',
    'Owner':'',
    'AccountingGroup':'',
    'ImageSize_RAW':0.,
    'DiskUsage_RAW':0.,
    'ExecutableSize_RAW':0.,
    'BytesSent':0.,
    'BytesRecvd':0.,
    'ResidentSetSize_RAW':0.,
    'RequestCpus':1.,
    'Requestgpus':0.,
    'RequestMemory':1000.,
    'RequestDisk':1000000.,
    'NumJobStarts':0.,
    'NumShadowStarts':0.,
    'GlobalJobId':'',
    'ClusterId':0.,
    'ProcId':0.,
    'ExitBySignal':False,
    'ExitCode':0.,
    'ExitSignal':0.,
    'ExitStatus':0.,
    'CumulativeSlotTime':0.,
    'LastRemoteHost':'',
    'QDate':now,
    'JobStartDate':now,
    'JobCurrentStartDate':now,
    'EnteredCurrentStatus':now,
    'RemoteUserCpu':0.,
    'RemoteSysCpu':0.,
    'CompletionDate':now,
    'CommittedTime':0.,
    'RemoteWallClockTime':0.,
    'MATCH_EXP_JOBGLIDEIN_ResourceName':'other',
    'StartdPrincipal':'',
    'DAGManJobId':0.,
    'LastJobStatus':0.,
    'LastHoldReason':'',
    'LastRemotePool':'',
}

key_types = {
    'number': ['AutoClusterId','BlockReadBytes','BlockReadKbytes','BlockReads',
               'BlockWriteBytes','BlockWriteKbytes','BufferBlockSize','BufferSize',
               'BytesRecvd','BytesSent','ClusterId','CommittedSlotTime','CommittedSuspensionTime',
               'CommittedTime','CompletionDate','CoreSize','CumulativeSlotTime','CumulativeSuspensionTime',
               'CurrentHosts','DiskUsage','DiskUsage_RAW','EnteredCurrentStatus','ExecutableSize',
               'ExecutableSize_RAW','ExitCode','ExitStatus','ImageSize','ImageSize_RAW',
               'JobCurrentStartDate','JobCurrentStartExecutingDate','JobFinishedHookDone',
               'JobLeaseDuration','JobLeaseExpiration','JobNotification','JobPrio','JobRunCount','JobStartDate',
               'JobStatus','JobUniverse','LastJobLeaseRenewal','LastJobStatus','LastMatchTime',
               'LastSuspensionTime','LocalSysCpu','LocalUserCpu','MachineAttrCpus0','MachineAttrSlotWeight0',
               'MaxHosts','MinHosts','NumCkpts','NumCkpts_RAW','NumJobMatches','NumJobStarts',
               'NumRestarts','NumShadowStarts','NumSystemHolds','OrigMaxHosts','ProcId','QDate',
               'Rank','RecentBlockReadBytes','RecentBlockReadKbytes','RecentBlockReads',
               'RecentBlockWriteBytes','RecentBlockWriteKbytes','RecentBlockWrites',
               'RecentStatsLifetimeStarter','RecentStatsTickTimeStarter','RecentWindowMaxStarter',
               'RemoteSysCpu','RemoteUserCpu','RemoteWallClockTime','RequestCpus','RequestDisk',
               'RequestMemory','Requestgpus','ResidentSetSize','ResidentSetSize_RAW',
               'StatsLastUpdateTimeStarter','StatsLifetimeStarter','TotalSuspensions',
               'TransferInputSizeMB'],
    'bool': ['EncryptExecuteDirectory','ExitBySignal','LeaveJobInQueue','NiceUser','OnExitHold',
             'OnExitRemove','PeriodicHold','PeriodicRelease','StreamErr','StreamOut',
             'TerminationPending','TransferIn','WantCheckpoint','WantRemoteIO',
             'WantRemoteSyscalls','wantglidein','wantrhel6'],
}

class ElasticClient(object):
    def __init__(self, hostname, basename):
        self.session = requests.Session()
        # try a connection
        r = self.session.get(hostname)
        r.raise_for_status()
        # concat hostname and basename
        self.hostname = hostname+'/'+basename+'/'
    def put(self, name, index_name, data):
        r = None
        try:
            r = self.session.put(self.hostname+name+'/'+index_name, json=data)
            r.raise_for_status()
        except Exception:
            logging.warn('cannot put %s/%s to elasticsearch at %r', name,
                         index_name, self.hostname, exc_info=True)
            if r:
                logging.warn('%r',r.content)
            raise

parser = OptionParser('usage: %prog [options] history_files')
parser.add_option('-a','--address',help='elasticsearch address')
parser.add_option('-b','--basename',default='condor',
                  help='collection basename (default condor)')
parser.add_option('-n','--indexname',default='job_history',
                  help='index name (default job_history)')
parser.add_option('--collectors', default=False, action='store_true',
                  help='Args are collector addresses, not files')
(options, args) = parser.parse_args()
if not args:
    parser.error('no condor history files')

client = ElasticClient(options.address, options.basename)


reserved_ips = {
    '18.12': 'MIT',
    '23.22': 'AWS',
    '35.9': 'MSU',
    '40.78': 'Azure',
    '40.112': 'Azure',
    '50.16': 'AWS',
    '50.17': 'AWS',
    '54.144': 'AWS',
    '54.145': 'AWS',
    '54.157': 'AWS',
    '54.158': 'AWS',
    '54.159': 'AWS',
    '54.161': 'AWS',
    '54.163': 'AWS',
    '54.166': 'AWS',
    '54.167': 'AWS',
    '54.197': 'AWS',
    '54.204': 'AWS',
    '54.205': 'AWS',
    '54.211': 'AWS',
    '54.227': 'AWS',
    '54.243': 'AWS',
    '72.36': 'Illinois',
    '128.9': 'osgconnect',
    '128.55': 'Berkeley',
    '128.84': 'NYSGRID_CORNELL_NYS1',
    '128.104': 'CHTC',
    '128.105': 'CHTC',
    '128.118': 'Bridges',
    '128.120': 'UCD',
    '128.205': 'osgconnect',
    '128.211': 'Purdue-Hadoop',
    '128.227': 'FLTech',
    '128.230': 'Syracuse',
    '129.74': 'NWICG_NDCMS',
    '129.93': 'Nebraska',
    '129.105': 'NUMEP-OSG',
    '129.107': 'UTA_SWT2',
    '129.119': 'SU-OG',
    '129.128': 'illume',
    '129.130': 'Kansas',
    '129.217': 'LIDO_Dortmund',
    '130.74': 'Miss',
    '130.127': 'Clemson-Palmetto',
    '130.199': 'BNL-ATLAS',
    '131.94': 'FLTECH',
    '131.215': 'CIT_CMS_T2',
    '131.225': 'USCMS-FNAL-WC1',
    '132.206': 'CA-MCGILL-CLUMEQ-T2',
    '133.82': 'Chiba',
    '134.93': 'mainz',
    '136.145': 'osgconnect',
    '137.99': 'UConn-OSG',
    '137.135': 'Azure',
    '138.23': 'UCRiverside',
    '138.91': 'Azure',
    '141.34': 'DESY-HH',
    '142.150': 'CA-SCINET-T2',
    '142.244': 'Alberta',
    '144.92': 'HEP_WISC',
    '149.165': 'Indiana',
    '155.101': 'Utah',
    '163.118': 'FLTECH',
    '169.228': 'UCSDT2',
    '171.67': 'HOSTED_STANFORD',
    '174.129': 'AWS',
    '184.73': 'AWS',
    '192.5': 'Boston',
    '192.12': 'Colorado',
    '192.41': 'AGLT2',
    '192.84': 'Ultralight',
    '192.168': None,
    '192.170': 'MWT2',
    '193.58': 'T2B_BE_IIHE',
    '193.190': 'T2B_BE_IIHE',
    '198.32': 'osgconnect',
    '198.48': 'Hyak',
    '198.202': 'Comet',
    '200.136': 'SPRACE',
    '200.145': 'SPRACE',
    '206.12': 'CA-MCGILL-CLUMEQ-T2',
    '216.47': 'MWT2',
}
reserved_ips.update({'10.%d'%i:None for i in range(256)})
reserved_ips.update({'172.%d'%i:None for i in range(16,32)})

reserved_domains = {
    'aglt2.org': 'AGLT2',
    'bridges.psc.edu': 'Bridges',
    'campuscluster.illinois.edu': 'Illinois',
    'cl.iit.edu': 'MWT2',
    'cm.cluster': 'LIDO_Dortmund',
    'cmsaf.mit.edu': 'MIT',
    'colorado.edu': 'Colorado',
    'cpp.ualberta.ca': 'Alberta',
    'crc.nd.edu': 'NWICG_NDCMS',
    'cci.wisc.edu': 'CHTC',
    'chtc.wisc.edu': 'CHTC',
    'cs.wisc.edu': 'CS_WISC',
    'cse.buffalo.edu': 'osgconnect',
    'discovery.wisc.edu': 'CHTC',
    'ec2.internal': 'AWS',
    'ember.arches': 'Utah',
    'fnal.gov': 'USCMS-FNAL-WC1',
    'grid.tu-dortmund.de': 'LIDO_Dortmund',
    'guillimin.clumeq.ca': 'Guillimin',
    'hcc.unl.edu': 'Crane',
    'hep.caltech.edu': 'CIT_CMS_T2',
    'hep.int': 'osgconnect',
    'hep.olemiss.edu': 'Miss',
    'hep.wisc.edu': 'HEP_WISC',
    'icecube.wisc.edu': 'NPX',
    'ics.psu.edu': 'Bridges',
    'iihe.ac.be': 'T2B_BE_IIHE',
    'illume.systems': 'illume',
    'internal.cloudapp.net': 'osgconnect',
    'isi.edu': 'osgconnect',
    'iu.edu': 'Indiana',
    'lidocluster.hp': 'LIDO_Dortmund',
    'math.wisc.edu': 'MATH_WISC',
    'mwt2.org': 'MWT2',
    'msu.edu': 'MSU',
    'nut.bu.edu': 'Boston',
    'palmetto.clemson.edu': 'Clemson-Palmetto',
    'panther.net': 'FLTECH',
    'phys.uconn.edu': 'UConn-OSG',
    'rcac.purdue.edu': 'Purdue-Hadoop',
    'research.northwestern.edu': 'NUMEP-OSG',
    'sdsc.edu': 'Comet',
    'stat.wisc.edu': 'CHTC',
    't2.ucsd.edu': 'UCSDT2',
    'tier3.ucdavis.edu':'UCD',
    'unl.edu': 'Nebraska',
    'uppmax.uu.se': 'Uppsala',
    'usatlas.bnl.gov': 'BNL-ATLAS',
    'wisc.cloudlab.us': 'CLOUD_WISC',
    'wisc.edu': 'WISC',
    'zeuthen.desy.de': 'DESY-ZN',
}

site_names = {
    'DESY-ZN': 'DE-DESY',
    'DESY-HH': 'DE-DESY',
    'DESY': 'DE-DESY',
    'Brussels': 'BE-IIHE',
    'T2B_BE_IIHE': 'BE-IIHE',
    'BEgrid-ULB-VUB': 'BE-IIHE',
    'Guillimin': 'CA-McGill',
    'CA-MCGILL-CLUMEQ-T2': 'CA-McGill',
    'mainz': 'DE-Mainz',
    'mainzgrid': 'DE-Mainz',
    'CA-SCINET-T2': 'CA-Toronto',
    'Alberta': 'CA-Alberta',
    'parallel': 'CA-Alberta',
    'jasper': 'CA-Alberta',
    'illume': 'CA-Alberta',
    'RWTH-Aachen': 'DE-Aachen',
    'aachen': 'DE-Aachen',
    'wuppertalprod': 'DE-Wuppertal',
    'Uppsala': 'SE-Uppsala',
    'Bartol': 'US-Bartol',
    'UNI-DORTMUND': 'DE-Dortmund',
    'LIDO_Dortmund': 'DE-Dortmund',
    'PHIDO_Dortmund': 'DE-Dortmund',
    'UKI-NORTHGRID-MAN-HEP': 'UK-Manchester',
    'UKI-LT2-QMUL': 'UK-Manchester',
    'Bridges': 'XSEDE-Bridges',
    'Comet': 'XSEDE-Comet',
    'HOSTED_STANFORD': 'XSEDE-XStream',
    'Xstream': 'XSEDE-XStream',
    'NPX': 'US-NPX',
    'GZK': 'US-GZK',
    'CHTC': 'US-CHTC',
    'Marquette': 'US-Marquette',
    'UMD': 'US-UMD',
    'Japan': 'JP-Chiba',
    'Chiba': 'JP-Chiba',
}

def get_site_from_domain(hostname):
    parts = hostname.lower().split('.')
    ret = None
    if '.'.join(parts[-3:]) in reserved_domains:
        ret = reserved_domains['.'.join(parts[-3:])]
    elif '.'.join(parts[-2:]) in reserved_domains:
        ret = reserved_domains['.'.join(parts[-2:])]
    return ret

def get_site_from_ip_range(ip):
    parts = ip.split('.')
    ret = None
    if len(parts) == 4 and all(p.isdigit() for p in parts):
        if '.'.join(parts[:2]) in reserved_ips:
            ret = reserved_ips['.'.join(parts[:2])]
    return ret


def get_type(val):
    if val == 'false':
        return False
    elif val == 'true':
        return True
    elif val.startswith('"') and val.endswith('"') and '"' not in val[1:-1]:
        return val[1:-1]
    try:
        return int(val)
    except:
        try:
            return float(val)
        except:
            return val

def filter_keys(data):
    for k in data.keys():
        if k not in good_keys:
            del data[k]
    for k in good_keys:
        if k not in data:
            data[k] = good_keys[k]
        if isinstance(good_keys[k],bool):
            try:
                data[k] = bool(data[k])
            except:
                data[k] = good_keys[k]
        elif isinstance(good_keys[k],(float,int)):
            try:
                data[k] = float(data[k])
            except:
                data[k] = good_keys[k]
        elif isinstance(good_keys[k],datetime):
            try:
                data[k] = datetime.utcfromtimestamp(data[k]).isoformat()
            except:
                data[k] = zero
        else:
            data[k] = str(data[k])

def fix_types(data):
    for t in key_types:
        if t == 'string':
            operation = str
        elif t == 'number':
            operation = float
        elif t == 'bool':
            operation = bool
        else:
            raise Exception('unknown type %r'%t)
        for k in key_types[t]:
            if k not in data:
                continue
            try:
                data[k] = operation(data[k])
            except:
                logging.info("dropping bad key %r", k, exc_info=True)
                del data[k]

def is_bad_site(data):
    if 'MATCH_EXP_JOBGLIDEIN_ResourceName' not in data:
        return True
    site = data['MATCH_EXP_JOBGLIDEIN_ResourceName']
    if site in ('other','osgconnect','xsede-osg','WIPAC','wipac'):
        return True
    if '.' in site:
        return True
    if site.startswith('gzk9000') or site.startswith('gzk-'):
        return True
    return False

def insert(data):
    # fix site
    bad_site = is_bad_site(data)
    if bad_site:
        if 'LastRemoteHost' in data:
            site = get_site_from_domain(data['LastRemoteHost'].split('@')[-1])
            if site:
                data['MATCH_EXP_JOBGLIDEIN_ResourceName'] = site
            elif 'StartdPrincipal' in data:
                site = get_site_from_ip_range(data['StartdPrincipal'].split('/')[-1])
                if site:
                    data['MATCH_EXP_JOBGLIDEIN_ResourceName'] = site
    # add completion date
    if data['CompletionDate'] != zero and data['CompletionDate']:
        data['date'] = data['CompletionDate']
    elif data['EnteredCurrentStatus'] != zero and data['EnteredCurrentStatus']:
        data['date'] = data['EnteredCurrentStatus']
    else:
        data['date'] = datetime.utcnow().isoformat()
    # add used time
    if 'RemoteWallClockTime' in data:
        data['walltimehrs'] = data['RemoteWallClockTime']/3600.
    else:
        data['walltimehrs'] = 0.
    # add site
    if data['MATCH_EXP_JOBGLIDEIN_ResourceName'] in site_names:
        data['site'] = site_names[data['MATCH_EXP_JOBGLIDEIN_ResourceName']]
    else:
        data['site'] = 'other'
    # upload
    index_id = data['GlobalJobId'].replace('#','-').replace('.','-')
    try:
        client.put(options.indexname,index_id,data)
    except Exception:
        logging.warn('%r',data)

if options.collectors:
    # connect to condor collectors and schedds to pull history directly
    import htcondor
    start_dt = datetime.now()-timedelta(minutes=10)
    start_stamp = time.mktime(start_dt.timetuple())
    for coll_address in args:
        coll = htcondor.Collector(coll_address)
        schedd_ads = coll.locateAll(htcondor.DaemonTypes.Schedd)
        for schedd_ad in schedd_ads:
            print('getting history from', schedd_ad['Name'])
            schedd = htcondor.Schedd(schedd_ad)
            i = 0
            for i,entry in enumerate(schedd.history('EnteredCurrentStatus >= {0}'.format(start_stamp),[],10000)):
                ret = {}
                for k in entry.keys():
                    try:
                        ret[k] = entry.eval(k)
                    except TypeError:
                        ret[k] = entry[k]
                filter_keys(ret)
                #fix_types(ret)
                insert(ret)
            print('   got',i,'entries')

else:
    # get history from files
    for path in args:
        for filename in glob.iglob(path):
            with (gzip.open(filename) if filename.endswith('.gz') else open(filename)) as f:
                entry = ''
                for line in f.readlines():
                    if line.startswith('***'):
                        c = classad.parseOne(entry)
                        ret = {}
                        for k in c.keys():
                            try:
                                ret[k] = c.eval(k)
                            except TypeError:
                                ret[k] = c[k]
                        filter_keys(ret)
                        #fix_types(ret)
                        insert(ret)
                        entry = ''
                    else:
                        entry += line+'\n'
                        #entry[name.strip()] = get_type(value.strip())
            print('.',end='')

