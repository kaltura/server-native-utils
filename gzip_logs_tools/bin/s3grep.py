from optparse import OptionParser
import subprocess
import botocore
import datetime
import hashlib
import fnmatch
import os.path
import boto3
import json
import time
import zlib
import sys

FILTER_INCLUDE = 'I'
FILTER_EXCLUDE = 'E'

ZBLOCKGREP_BIN = '/opt/server-native-utils/gzip_logs_tools/zblockgrep/zblockgrep'
CRED_REFRESH = 1800

CRED_FILE = '/tmp/s3grep.%s.ini' % os.getpid()

CACHE_FILE = '/tmp/s3grep.%s.cache'
CACHE_EXPIRY = 1800


cred_last = None
def update_cred_ini(session):
    global cred_last

    cred = session.get_credentials()
    cred = cred.get_frozen_credentials()

    data = '[s3]\nregion=%s\naccess_key=%s\nsecret_key=%s\nsecurity_token=%s\n' % \
        (options.region, cred.access_key, cred.secret_key, cred.token)
    if data == cred_last:
        return

    if options.verbose:
        sys.stderr.write('Updating credentials\n')

    temp_cred_file = CRED_FILE + '.tmp'
    with open(temp_cred_file, 'wb') as f:
        f.write(data)
    os.rename(temp_cred_file, CRED_FILE)

    cred_last = data

def del_cred_ini():
    try:
        os.remove(CRED_FILE)
    except OSError:
        pass


def apply_filter(key, filter):
    base_key = os.path.basename(key)
    for type, pattern in filter:
        if fnmatch.fnmatchcase(base_key, pattern):
            return type == FILTER_INCLUDE
    return True

def list(s3, bucket_name, prefix, filter, delimiter):
    paginator = s3.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix,
        Delimiter=delimiter)

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

def is_wildcard(path):
    return '*' in path or '?' in path or '[' in path

def expand(s3, bucket_name, parts, base=[]):
    for i in range(len(parts)):
        part = parts[i]
        if i == len(parts) - 1 or not is_wildcard(part):
            base.append(part)
            continue

        prefix = ''.join(map(lambda x: x + '/', base))
        paginator = s3.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=bucket_name,
            Prefix=prefix, Delimiter='/')

        matches = []
        for page in page_iterator:
            if not 'CommonPrefixes' in page:
                continue

            for item in page['CommonPrefixes']:
                # extract the last part from the prefix
                key = item['Prefix'].split('/')[-2]
                if fnmatch.fnmatchcase(key, part):
                    matches.append(key)

        if len(matches) == 1:
            base.append(matches[0])
            continue

        # handle the remaining parts recursively
        result = []
        for match in matches:
            result += expand(s3, bucket_name, parts[(i + 1):], base + [match])
        return result

    return ['/'.join(base)]


def include_exclude(option, opt, value, parser, type):
    dest = option.dest
    values = parser.values
    values.ensure_value(dest, []).append((type, value))

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

done_size = 0
last_percent = 0
def update_progress(size):
    global done_size, last_percent

    if not options.verbose:
        return

    done_size += size
    percent = int(done_size * 100 / total_size)
    if percent != last_percent:
        sys.stderr.write('%s%% ' % percent)
        last_percent = percent


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
parser.add_option('-A', '--after-context', dest='after_context', type='int',
    help='Print NUM lines of trailing context after matching lines', metavar='NUM')
parser.add_option('-B', '--before-context', dest='before_context', type='int',
    help='Print NUM lines of leading context before matching lines', metavar='NUM')
parser.add_option('-C', '--context', dest='context', type='int',
    help='Print NUM lines of output context', metavar='NUM')
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

cache_key = [options.profile, bucket_name, prefix, options.filter, delimiter]
m = hashlib.md5()
m.update(json.dumps(cache_key))
cache_file = CACHE_FILE % m.hexdigest()

start = time.time()

try:
    cache_time = os.path.getmtime(cache_file)
except:
    cache_time = 0

if time.time() - cache_time < CACHE_EXPIRY:
    if options.verbose:
        sys.stderr.write('Loading from cache...\n')
    with open(cache_file, 'rb') as f:
        data = f.read()
    file_list = json.loads(zlib.decompress(data))
else:
    prefixes = expand(s3, bucket_name, prefix.split('/'))
    if options.verbose and prefixes != [prefix]:
        sys.stderr.write('Expanded prefixes: %s\n' % ', '.join(prefixes))

    if options.verbose:
        sys.stderr.write('Listing objects...\n')
    file_list = []
    for cur_prefix in prefixes:
        file_list += list(s3, bucket_name, cur_prefix, options.filter, delimiter)

    data = zlib.compress(json.dumps(file_list))
    with open(cache_file, 'wb') as f:
        f.write(data)

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

if (os.path.exists(ZBLOCKGREP_BIN) and not options.after_context
    and not options.before_context and not options.context):

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

    grep_options = " -T%s -f '%s'" % (options.max_processes, json.dumps(filter))
    if output_filename:
        grep_options += ' -H'
    else:
        grep_options += ' -h'

    if options.verbose:
        sys.stderr.write('Using: zblockgrep%s\n' % grep_options)
else:
    ZBLOCKGREP_BIN = False

    grep_options = ''
    if options.ignore_case:
        grep_options += ' -i'
    if options.after_context:
        grep_options += ' -A%s' % options.after_context
    if options.before_context:
        grep_options += ' -B%s' % options.before_context
    if options.context:
        grep_options += ' -C%s' % options.context
    if options.pcre:
        grep_options += ' -P'

    if options.verbose:
        sys.stderr.write('Using: grep%s\n' % grep_options)

# run the grep commands
if ZBLOCKGREP_BIN:
    for chunk in chunks(file_list, options.max_processes * 4):
        size = sum([x[0] for x in chunk])
        paths = ['s3://%s/%s' % (bucket_name, x[1]) for x in chunk]

        update_progress(size)

        update_cred_ini(session)
        cmd = '%s --ini %s %s %s' % (ZBLOCKGREP_BIN, CRED_FILE,
            grep_options, ' '.join(paths))
        if options.pipe is not None:
            cmd += ' | %s' % options.pipe

        os.system(cmd)

    del_cred_ini()

else:
    processes = set()
    for size, prefix in file_list:
        update_progress(size)

        path = 's3://%s/%s' % (bucket_name, prefix)

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

if options.verbose:
    sys.stderr.write('\nDone, took %s sec\n' % int(time.time() - start))
