#!/usr/bin/env python

import sys
import os
import logging
import psycopg2
import StringIO

COUNTERS = { 'bad_record_too_long':0, 'bad_record_too_short':0, 'good_record':0,
             'normalized_numbers':0, 'no_need_normalization':0,
             'bad_numbers':0
           }

logging.basicConfig(
    format='\t'.join([
        '%(asctime)s',
        '%(name)s',
        '%(levelname)s',
        '(%(process)d)',
        '%(message)s'
    ]),
    level=logging.WARNING
)

def extract_release_code(release_code_bytes):
    raw_bytes = release_code_bytes.split(':')
    if raw_bytes[1]:
        return int(bin(int(raw_bytes[1], 16)), 2) & int('0b01111111', 2)
    else:
        COUNTERS['bad_release_code']
        LOG.debug("Can't extract release code from %s", release_code_bytes)
    return 0

def normalize_number(msisdn):
    if len(msisdn) == 11:
        if msisdn.startswith('8', 0, 1):
            LOG.debug("Perform normalization %s->%s", msisdn, '7' + msisdn[1:])
            msisdn='7' + msisdn[1:]
            COUNTERS['normalized_numbers'] += 1
        else:
            LOG.debug("No need normalization for %s", msisdn)
            COUNTERS['no_need_normalization'] += 1
    elif len(msisdn) == 10:
        msisdn = '7' + msisdn
        COUNTERS['normalized_numbers'] += 1
    else:
        LOG.debug("Bad number found: %s", msisdn)
        COUNTERS['bad_numbers'] += 1
    return msisdn

def read_text_dump(text_file):
    global COUNTERS

    dirname=os.path.dirname(text_file)
    filename_bn=os.path.basename(text_file)

    if os.path.exists(dirname + '/.' + filename_bn):
        LOG.warning("Data for file %s, already uploaded", text_file)

    with open(text_file, 'r') as fh:
        for line in fh:
            arr = line.strip().split(',')
            if len(arr) == 6:
                if '' not in arr:
                    msisdns = arr[1:3]
                    normalized_numbers=list(map(normalize_number, msisdns))
                    COUNTERS['good_record'] += 1
                    yield ','.join([arr[0], ','.join(normalized_numbers), ','.join(arr[3:])])
                else:
                    COUNTERS['bad_record_too_short'] += 1
            else:
                COUNTERS['bad_record_too_long'] += 1

def database_upload(mem_file):
    conn = psycopg2.connect("dbname=camel user=camel host=localhost password=camel")
    cur = conn.cursor()
    cur.copy_from(mem_file, 'camel_data', sep=',')
    cur.close()
    conn.commit()
    conn.close()

def main():
    try:
        text_file = sys.argv[1]

        dirname=os.path.dirname(text_file)
        filename_bn=os.path.basename(text_file)

        if os.path.exists(dirname + '/.' + filename_bn):
            LOG.warning("Data for file %s, already uploaded", text_file)
            sys.exit(1)

        mem_file = StringIO.StringIO()
        for record in read_text_dump(sys.argv[1]):
            mem_file.write(record + "\n")
        mem_file.seek(0)
        database_upload(mem_file)
        mem_file.close()
        with open(dirname + '/.' + filename_bn, 'a'):
            pass
        print COUNTERS
    except IndexError:
        LOG.error("Please specify file to read")

if __name__=='__main__':
    LOG = logging.getLogger(os.path.basename(sys.argv[0]))
    main()
