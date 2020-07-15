#!/usr/bin/env python
__author__ = 'ben.slater@instaclustr.com'

from datadog import initialize
from time import sleep
from datadog import statsd
import requests, json
from requests.auth import HTTPBasicAuth
import os, sys
import json_logging, logging

configFile = os.getenv("CONFIG_FILE_PATH", os.path.dirname(os.path.realpath(__file__)) + "/configuration.json")
f = open(configFile)
configuration = json.loads(f.read())
f.close()

# Get creds from environment variables
if os.getenv("DD_API_KEY") is not None:
    configuration['dd_options']['api_key'] = os.getenv("DD_API_KEY")
if os.getenv("DD_APP_KEY") is not None:
    configuration['dd_options']['app_key'] = os.getenv("DD_APP_KEY")
if os.getenv("DD_STATSD_HOST") is not None:
    configuration['dd_options']['statsd_host'] = os.getenv("DD_STATSD_HOST")

if os.getenv("IC_USER_NAME") is not None:
    configuration['ic_options']['user_name'] = os.getenv("IC_USER_NAME")
if os.getenv("IC_API_KEY") is not None:
    configuration['ic_options']['api_key'] = os.getenv("IC_API_KEY")

# Logging setup
app_name = os.getenv('APP_NAME', 'instaclustr-monitor')
log_level = logging.getLevelName(os.getenv('LOG_LEVEL', 'INFO').upper())
json_logging.ENABLE_JSON_LOGGING = os.getenv('ENABLE_JSON_LOGGING', 'TRUE')
json_logging.init_non_web()
logger = logging.getLogger(app_name)
logger.setLevel(log_level)
logger.addHandler(logging.StreamHandler(sys.stdout))

dd_options = configuration['dd_options']
print("dd_options: {0}".format(dd_options))
initialize(**dd_options)

auth_details = HTTPBasicAuth(username=configuration['ic_options']['user_name'], password=configuration['ic_options']['api_key'])



consecutive_fails = 0
while True:
    response = requests.get(url="https://api.instaclustr.com/monitoring/v1/clusters/{0}?metrics={1},".format(configuration['cluster_id'], configuration['metrics_list']), auth=auth_details)

    if not response.ok:
        # got an error response from the Instaclustr API - raise an alert in DataDog after 3 consecutive fails
        consecutive_fails += 1
        print ("Error retrieving metrics from Instaclustr API: {0} - {1}".format(response.status_code, response.content))
        if consecutive_fails > 3:
            statsd.event("Instaclustr monitoring API error", "Error code is: {0}".format(response.status_code))
        sleep(20)
        continue

    consecutive_fails = 0
    metrics = json.loads(response.content)
    tag_list = []
    for node in metrics:
        if node["publicIp"] is not None:
            tag_list.append('ic_public_ip:' + node["publicIp"])
        if node["privateIp"] is not None:
            tag_list.append('ic_private_ip:' + node["privateIp"])
        tag_list.append('ic_rack_name' + node["rack"]["name"])
        tag_list.append('ic_data_centre_custom_name' + node["rack"]["dataCentre"]["customDCName"])
        tag_list.append('ic_data_centre_name' + node["rack"]["dataCentre"]["name"])
        data_centre_provider = node["rack"]["dataCentre"]["provider"]
        tag_list.append('ic_data_centre_provider' + data_centre_provider)
        tag_list.append('ic_provider_account_name' + node["rack"]["providerAccount"]["name"])
        tag_list.append('ic_provider_account_provider' + node["rack"]["providerAccount"]["provider"])

        if data_centre_provider == 'AWS_VPC':
            tag_list = tag_list + [
                'region:' + node["rack"]["dataCentre"]["name"].lower().replace("_", "-"),
                'availability_zone:' + node["rack"]["name"]
            ]

        for metric in node["payload"]:
            dd_metric_name = 'instaclustr.{0}'.format(metric["metric"])
            if metric["metric"] == "nodeStatus":
                # node status metric maps to a data dog service check
                if metric["values"][0]["value"] =="WARN":
                    statsd.service_check(dd_metric_name, 1, tags=configuration['tags'] + tag_list) # WARN status
                else:
                    statsd.service_check(dd_metric_name, 0, tags=configuration['tags'] + tag_list) # OK status
            else:
                # all other metrics map to a data dog guage
                statsd.gauge(dd_metric_name, metric["values"][0]["value"], tags=configuration['tags'] + tag_list)
                logger.debug("Metric {0} sent".format(dd_metric_name))
    logger.info("Metrics sent")
    sleep(20)
