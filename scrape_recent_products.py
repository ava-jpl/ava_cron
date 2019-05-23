#!/usr/bin/env python

'''
Submits CMR scraping job for most recent products
'''

from __future__ import print_function
import os
import json
import argparse
import requests
import datetime
import dateutil.parser
import submit_job

def submit(short_name, queue, job_version, priority, num_days):
    '''
    submits a job to mozart to start cmr scraping job
    '''
    #job_name = 'job-scrape_cmr_region:{}'.format(job_version)
    job_name = 'job-scrape_cmr_region'
    now = datetime.datetime.utcnow()
    start = (now - datetime.timedelta(days=int(num_days))).strftime('%Y-%m-%dT%H:%M:%SZ')
    end = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    job_params = {"short_name": "ASTL_L1T", "location": {"type":"Polygon","coordinates":[[[-179,-85],[-179,85],[179,85],[179,-85],[-179,-85]]]}, "starttime": start, "endtime": end}
    # submit mozart job
    job_submit_url = os.path.join(app.conf['MOZART_URL'], 'api/v0.2/job/submit')
    tags = 'SCRAPE_RECENT_{}'.format(short_name)
    #submit the job
    submit_job.main(job_name, job_params, job_version, queue, priority, tags) 


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-s', '--short_name', help='product short_name', dest='short_name', required=True)
    parser.add_argument('-v', '--version', help='release version, eg "master" or "release-20180615"', dest='version', required=False, default='master')
    parser.add_argument('-q', '--queue', help='Job queue', dest='queue', required=False, default='factotum-job_worker-small')
    parser.add_argument('-p', '--priority', help='Job priority', dest='priority', required=False, default='5')
    parser.add_argument('-n', '--num_days', help='Number of days to lookback over', dest='num_days', required=False, default='12')
    args = parser.parse_args()
    submit(args.short_name, args.queue, args.job_version, args.priority, args.num_days)
