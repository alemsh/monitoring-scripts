import os
import glob
import argparse
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('files', nargs='+', help='files to process')
parser.add_argument('--env', default='/mnt/lfss/condor_logs/venv.sh', help='script to load env')
parser.add_argument('-e','--executable', help='executable and common args')
parser.add_argument('-s','--scratch', help='scratch dir')
parser.add_argument('-j','--maxjobs', default=20, help='max concurrent jobs')
args = parser.parse_args()

files = []
for f in args.files:
    files.extend(glob.glob(os.path.abspath(f)))

if not os.path.exists(args.scratch):
    os.makedirs(args.scratch)

dagpath = os.path.join(args.scratch, 'dag')
condorpath = os.path.join(args.scratch, 'condor')

with open(condorpath, 'w') as f:
    f.write('executable = {}\n'.format(args.env))
    f.write('arguments = {} $(FILE)\n'.format(args.executable))
    f.write("""output = out
error = /dev/null
log = /dev/null
notification = never
+FileSystemDomain = "blah"
request_memory = 2500
queue
""")

with open(dagpath, 'w') as f:
    for i,file in enumerate(files):
        f.write('JOB job{} condor\n'.format(i))
        f.write('VARS job{} FILE="{}"\n'.format(i,file))

cmd = ['condor_submit_dag','-maxjobs', str(args.maxjobs), dagpath]
print(cmd)
subprocess.check_call(cmd, cwd=args.scratch)
