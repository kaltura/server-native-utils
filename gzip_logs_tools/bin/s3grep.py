from optparse import OptionParser
import multiprocessing
import subprocess
import threading
import botocore
import datetime
import hashlib
import fnmatch
import boto3
import json
import time
import zlib
import sys
import os

FILTER_INCLUDE = 'I'
FILTER_EXCLUDE = 'E'

ZBLOCKGREP_BIN = '/opt/server-native-utils/gzip_logs_tools/zblockgrep/zblockgrep'
CRED_REFRESH = 1800

CRED_FILE = '/tmp/s3grep.%s.ini' % os.getpid()

CACHE_FILE = '/tmp/s3grep.%s.cache'
CACHE_EXPIRY = 1800

def write_error(msg):
    sys.stderr.write(msg)
    sys.stderr.flush()

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
        write_error('Updating credentials\n')

    temp_cred_file = CRED_FILE + '.tmp'
    with open(temp_cred_file, 'wb') as f:
        f.write(data.encode('utf8'))
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

def list(s3, bucket_name, prefix, filter, delimiter, result):
    paginator = s3.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix,
        Delimiter=delimiter)

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
            result.append((size, 's3://%s/%s' % (bucket_name, key)))

def is_wildcard(path):
    return '*' in path or '?' in path or '[' in path

def expand(s3, bucket_name, parts, base):
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

    return [(bucket_name, '/'.join(base))]


def include_exclude(option, opt, value, parser, type):
    dest = option.dest
    values = parser.values
    values.ensure_value(dest, []).append((type, value))

def chunks(lst, n):
    chunk = []
    for i in lst:
        chunk.append(i)
        if len(chunk) < n:
            continue
        yield chunk
        chunk = []
    if len(chunk) > 0:
        yield chunk

total_size = None
def print_file_list_info(file_list, start):
    global total_size

    if not options.verbose:
        return

    total_size = sum(map(lambda x: x[0], file_list))
    write_error('Took %s sec... scanning %s objects, %s MB...\n' % (
        int(time.time() - start), len(file_list), total_size // (1024 * 1024)))

done_size = 0
last_percent = 0
def update_progress(size):
    global done_size, last_percent, total_size

    done_size += size
    if not options.verbose or total_size is None:
        return

    percent = int(done_size * 100 / total_size)
    if percent != last_percent:
        write_error('%s%% ' % percent)
        last_percent = percent

def list_files_thread(file_list):
    if options.verbose:
        write_error('Listing objects...\n')

    for bucket_name, prefix in paths:
        list(s3, bucket_name, prefix, options.filter, delimiter, file_list)

    data = zlib.compress(json.dumps(file_list).encode('utf8'))
    with open(cache_file, 'wb') as f:
        f.write(data)

    print_file_list_info(file_list, start)

def files_iter(file_list, t):
    pos = 0
    while True:
        if pos < len(file_list):
            yield file_list[pos]
            pos += 1
            continue
        if not t.is_alive():
            break
        time.sleep(1)

def get_file_list_generator():
    file_list = []

    t = threading.Thread(target=list_files_thread, args=[file_list])
    t.daemon = True
    t.start()

    # wait in order to find out whether we have more than one file
    while len(file_list) <= 1 and t.is_alive():
        time.sleep(1)

    return files_iter(file_list, t), len(file_list) > 1


parser = OptionParser(usage='%prog [OPTION]... PATTERN S3URI1 [S3URI2 ...]',
    add_help_option=False)
parser.add_option('--help', help='display this help and exit', action='help')
parser.add_option('-v', '--verbose', dest='verbose', action='store_true',
    help='generate more verbose output')

# aws options
parser.add_option('-f', '--profile', dest='profile', default=None,
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
parser.add_option('--block-pattern', dest='block_pattern', default='^.',
    help='regular expression for identifying block start [default: %default]',
    metavar='PATTERN')

max_processes = min(multiprocessing.cpu_count(), 4)

parser.add_option('-m', '--max-processes', dest='max_processes', type='int',
    default=max_processes, help='maximum number of processes to spawn [default: %default]',
    metavar='COUNT')
parser.add_option('-p', '--pipe', dest='pipe',
    help='a command string to pipe the output to', metavar='CMD')
parser.add_option('-e', '--encoding', dest='encoding', default='gz',
    help='encoding - txt/gz [default: %default]', metavar='ENCODING')

# parse the command line
(options, args) = parser.parse_args()

if len(args) < 2:
    parser.error('expecting at least 2 arguments')

pattern = args[0]
s3_uris = args[1:]

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

cache_key = [options.profile, s3_uris, options.filter, delimiter]
m = hashlib.md5()
m.update(json.dumps(cache_key).encode('utf8'))
cache_file = CACHE_FILE % m.hexdigest()

start = time.time()

try:
    cache_time = os.path.getmtime(cache_file)
except:
    cache_time = 0

if time.time() - cache_time < CACHE_EXPIRY:
    if options.verbose:
        write_error('Loading from cache...\n')
    with open(cache_file, 'rb') as f:
        data = f.read()
    file_list = json.loads(zlib.decompress(data).decode('utf8'))
    multi_file = len(file_list) > 1
    print_file_list_info(file_list, start)
else:
    paths = []
    for s3_uri in s3_uris:
        if s3_uri.startswith('s3://'):
            s3_uri = s3_uri[len('s3://'):]
        bucket_name, prefix = s3_uri.split('/', 1)

        paths += expand(s3, bucket_name, prefix.split('/'), [])

    if options.verbose:
        str_paths = ['s3://%s/%s' % (x[0], x[1]) for x in paths]
        write_error('Searching paths: %s\n' % ' '.join(str_paths))

    file_list, multi_file = get_file_list_generator()

# init
if options.with_filename:
    output_filename = True
elif options.no_filename:
    output_filename = False
else:
    output_filename = multi_file

if (os.path.exists(ZBLOCKGREP_BIN) and not options.after_context
    and not options.before_context and not options.context
    and options.encoding == 'gz'):

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

    grep_options = " -T%s -f '%s' -p '%s'" % (options.max_processes,
        json.dumps(filter), options.block_pattern)
    if output_filename:
        grep_options += ' -H'
    else:
        grep_options += ' -h'

    if options.verbose:
        write_error('Using: zblockgrep%s\n' % grep_options)
else:
    ZBLOCKGREP_BIN = False

    grep_options = ' -a'
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

    if options.encoding == 'txt':
        decode = ''
    elif options.encoding == 'gz':
        decode = 'gzip -d | '
    else:
        parser.error('unknown encoding "%s"' % options.encoding)

    if options.verbose:
        write_error('Using: grep%s\n' % grep_options)

# run the grep commands
if ZBLOCKGREP_BIN:
    for chunk in chunks(file_list, options.max_processes * 4):
        size = sum([x[0] for x in chunk])
        paths = [x[1] for x in chunk]

        update_progress(size)

        update_cred_ini(session)
        cmd = '%s --ini %s %s %s' % (ZBLOCKGREP_BIN, CRED_FILE,
            grep_options, ' '.join(paths))
        if options.pipe is not None:
            cmd += ' | %s' % options.pipe

        p = subprocess.Popen(cmd, shell=True)
        p.wait()

    del_cred_ini()

else:
    processes = set()
    for size, path in file_list:
        update_progress(size)

        grep_command = 'grep' + grep_options
        if output_filename:
            grep_command += " '--label=%s' -H" % path
        grep_command += " '%s'" % pattern
        if options.pipe is not None:
            grep_command += ' | %s' % options.pipe

        profile_param = ''
        if options.profile is not None:
            profile_param = "--profile '%s'" % options.profile

        cmd = "aws s3 cp %s '%s' - | %s%s" % (
            profile_param, path, decode, grep_command)

        processes.add(subprocess.Popen(cmd, shell=True))

        if len(processes) < options.max_processes:
            continue

        os.wait()
        processes.difference_update([
            p for p in processes if p.poll() is not None])

    for p in processes:
        p.wait()

if options.verbose:
    write_error('\nDone, took %s sec\n' % int(time.time() - start))

