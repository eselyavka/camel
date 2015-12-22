#!/usr/bin/env python

import sys
import os
import logging
import psycopg2
import tempfile

logging.basicConfig(
    format='\t'.join([
        '%(asctime)s',
        '%(name)s',
        '%(levelname)s',
        '(%(process)d)',
        '%(message)s'
    ]),
    level=logging.INFO
)

def database_request(request_type, interval='1800'):
    conn = psycopg2.connect("dbname=camel user=camel host=localhost password=camel")
    if request_type == 'count':
        with open('count_changedir.sql', 'r') as fh:
            sql = fh.read()

        cur = conn.cursor()
        cur.execute(sql.replace('@REPLACEMENT@', interval))
        print cur.fetchone()[0]
        cur.close()
        conn.commit()
    elif request_type == 'all':
        tmp_file = tempfile.NamedTemporaryFile(mode='w+',delete=False)
        LOG.info("Will write file to %s", tmp_file.name)
        with open('all_except_changedir.sql', 'r') as fh:
            sql = fh.read()
        cur = conn.cursor()
        cur.execute(sql.replace('@REPLACEMENT@', interval))
        for data in cur:
            tmp_file.write(','.join([str(data[0]), ','.join(data[1:3]), str(data[3]), str(data[4]), str(data[5])]) + "\n")
        cur.close()
        conn.commit()
        tmp_file.close()
    else:
        LOG.ERROR("Request %s type is not supported", request_type)
    conn.close()

def main():
    database_request(sys.argv[1], sys.argv[2])

if __name__=='__main__':
    LOG = logging.getLogger(os.path.basename(sys.argv[0]))
    main()