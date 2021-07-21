from optparse import OptionParser
import subprocess
import botocore
import datetime
import fnmatch
import os.path
import boto3
import json
import time
import sys

FILTER_INCLUDE = 'I'
FILTER_EXCLUDE = 'E'

ZBLOCKGREP_BIN = '/opt/server-native-utils/gzip_logs_tools/zblockgrep/zblockgrep'
CRED_REFRESH = 1800

cred_file = '/tmp/s3.%s.ini' % os.getpid()
cred_last = None

def get_cred_ini(session):
    global cred_file, cred_last

    cred = session.get_credentials()
    cred = cred.get_frozen_credentials()

    data = '[s3]\nregion=%s\naccess_key=%s\nsecret_key=%s\nsecurity_token=%s\n' % \
        (options.region, cred.access_key, cred.secret_key, cred.token)
    if data == cred_last:
        return cred_file

    if options.verbose:
        sys.stderr.write('Updating credentials\n')

    temp_cred_file = cred_file + '.tmp'
    with open(temp_cred_file, 'wb') as f:
        f.write(data)
    os.rename(temp_cred_file, cred_file)

    cred_last = data
    return cred_file

def del_cred_ini():
    global cred_file

    if cred_file is not None:
        os.remove(cred_file)

def apply_filter(key, filter):
    base_key = os.path.basename(key)
    for type, pattern in filter:
        if fnmatch.fnmatchcase(base_key, pattern):
            return type == FILTER_INCLUDE
    return True

def list(s3, bucket_name, prefix, filter, delimiter):
    paginator = s3.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix,
        Delimiter='')

    result = []
    for page in page_iterator:
        if not 'Contents' in page:
            continue

        for item in page['Contents']:
            key = item['Key']
            size = item['Size']
            if size < options.min_size:
                continue
            if not apply_filter(key, filter):
                continue
            result.append((size, key))

    return result

def include_exclude(option, opt, value, parser, type):
    dest = option.dest
    values = parser.values
    values.ensure_value(dest, []).append((type, value))

parser = OptionParser(usage='%prog [OPTION]... PATTERN S3URI',
    add_help_option=False)
parser.add_option('--help', help='display this help and exit', action='help')
parser.add_option('-v', '--verbose', dest='verbose', action='store_true',
    help='generate more verbose output')

# aws options
parser.add_option('-f', '--profile', dest='profile', default='buckets',
    help='aws profile name [default: %default]', metavar='PROFILE')
parser.add_option('-R', '--region', dest='region', default='us-east-1',
    help='aws region name [default: %default]')

# file list options
parser.add_option('-I', '--include', dest='filter', type='string',
    action='callback', callback=include_exclude,
    callback_kwargs={'type': FILTER_INCLUDE},
    help='include objects that match the specified file pattern',
    metavar='PATTERN')
parser.add_option('-E', '--exclude', dest='filter', type='string',
    action='callback', callback=include_exclude,
    callback_kwargs={'type': FILTER_EXCLUDE},
    help='exclude objects that match the specified file pattern',
    metavar='PATTERN')
parser.add_option('-r', '--recursive', dest='recursive', action='store_true',
    help='scan all objects under the specified prefix')
parser.add_option('-M', '--min-size', dest='min_size', type='int', default=30,
    help='minimum file size to grep [default: %default]', metavar='BYTES')

# grep options
parser.add_option('-h', '--no-filename', dest='no_filename',
    action='store_true', help='suppress the file name prefix on output')
parser.add_option('-H', '--with-filename', dest='with_filename',
    action='store_true', help='print the file name for each match')
parser.add_option('-i', '--ignore-case', dest='ignore_case', default=False,
    action='store_true', help='ignore case distinctions')
parser.add_option('-P', '--perl-regexp', dest='pcre', default=False,
    action='store_true', help='PATTERN is a Perl regular expression')

parser.add_option('-m', '--max-processes', dest='max_processes', type='int',
    default=4, help='maximum number of processes to spawn [default: %default]',
    metavar='COUNT')
parser.add_option('-p', '--pipe', dest='pipe',
    help='a command string to pipe the output to', metavar='CMD')

# parse the command line
(options, args) = parser.parse_args()

if len(args) != 2:
    parser.error('expecting 2 arguments')

pattern, s3_uri = args

if s3_uri.startswith('s3://'):
    s3_uri = s3_uri[len('s3://'):]
bucket_name, prefix = s3_uri.split('/', 1)

if options.filter is None:
    options.filter = []
else:
    options.filter = options.filter[::-1]
    if options.filter[-1][0] == FILTER_INCLUDE:
        options.filter.append((FILTER_EXCLUDE, '*'))

# list the files
session = boto3.Session(profile_name=options.profile)
s3 = session.client('s3')

delimiter = '' if options.recursive else '/'
if options.verbose:
    sys.stderr.write('Listing objects...\n')
start = time.time()
file_list = list(s3, bucket_name, prefix, options.filter, delimiter)

if options.verbose:
    total_size = sum(map(lambda x: x[0], file_list))
    sys.stderr.write('Took %s sec... scanning %s objects, %s MB...\n' % (
        int(time.time() - start), len(file_list), total_size / (1024 * 1024)))

# init
if options.with_filename:
    output_filename = True
elif options.no_filename:
    output_filename = False
else:
    output_filename = len(file_list) > 1

if os.path.exists(ZBLOCKGREP_BIN):
    if options.verbose:
        sys.stderr.write('Using zblockgrep...\n')

    if options.pcre:
        filter = {
            'type': 'regex',
            'pattern': pattern,
            'ignorecase': options.ignore_case
        }
    else:
        filter = {
            'type': 'match',
            'text': pattern,
            'ignorecase': options.ignore_case
        }

    grep_options = " -f '%s'" % json.dumps(filter)
    if output_filename:
        grep_options += ' -H'
else:
    if options.verbose:
        sys.stderr.write('Using grep...\n')

    ZBLOCKGREP_BIN = False

    grep_options = ''
    if options.ignore_case:
        grep_options += ' -i'
    if options.pcre:
        grep_options += ' -P'


# run the grep commands
processes = set()
done_size = 0
last_percent = 0
for size, prefix in file_list:
    if options.verbose:
        done_size += size
        percent = int(done_size * 100 / total_size)
        if percent != last_percent:
            sys.stderr.write('%s%% ' % percent)
            last_percent = percent

    path = 's3://%s/%s' % (bucket_name, prefix)

    if ZBLOCKGREP_BIN:
        cmd = '%s --ini %s %s %s' % (ZBLOCKGREP_BIN, get_cred_ini(session),
            grep_options, path)
        if options.pipe is not None:
            cmd += ' | %s' % options.pipe
    else:
        grep_command = 'grep' + grep_options
        if output_filename:
            grep_command += " '--label=%s' -H" % path
        grep_command += " '%s'" % pattern
        if options.pipe is not None:
            grep_command += ' | %s' % options.pipe
        cmd = "aws s3 cp --profile '%s' '%s' - | gzip -d | %s" % (
            options.profile, path, grep_command)

    processes.add(subprocess.Popen(cmd, shell=True))

    if len(processes) < options.max_processes:
        continue

    os.wait()
    processes.difference_update([
        p for p in processes if p.poll() is not None])

for p in processes:
    p.wait()

del_cred_ini()

if options.verbose:
    sys.stderr.write('\nDone, took %s sec\n' % int(time.time() - start))
