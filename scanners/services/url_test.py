import boto3
import json
import time as pytime

if input("are you sure you want to run this script? (y/n)") != "y":
    exit()

inputs = [
    {
      "startdate": "2022-07-01",
      "enddate": "2022-07-14",
      "scanner": "PushPercScanner",
      "folder": "eod"
    },
    {
      "startdate": "2022-01-01",
      "enddate": "2022-06-30",
      "scanner": "PushPercScanner",
      "folder": "eod"
    },
    # {
    #   "startdate": "2021-07-01",
    #   "enddate": "2021-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2021-01-01",
    #   "enddate": "2021-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2020-07-01",
    #   "enddate": "2020-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2020-01-01",
    #   "enddate": "2020-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2019-07-01",
    #   "enddate": "2019-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2019-01-01",
    #   "enddate": "2019-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2018-07-01",
    #   "enddate": "2018-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2018-01-01",
    #   "enddate": "2018-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2017-07-01",
    #   "enddate": "2017-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2017-01-01",
    #   "enddate": "2017-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2016-07-01",
    #   "enddate": "2016-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2016-01-01",
    #   "enddate": "2016-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2015-07-01",
    #   "enddate": "2015-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2015-01-01",
    #   "enddate": "2015-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2014-07-01",
    #   "enddate": "2014-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2014-01-01",
    #   "enddate": "2014-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2013-07-01",
    #   "enddate": "2013-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2013-01-01",
    #   "enddate": "2013-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2012-07-01",
    #   "enddate": "2012-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2012-01-01",
    #   "enddate": "2012-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2011-07-01",
    #   "enddate": "2011-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2011-01-01",
    #   "enddate": "2011-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2010-07-01",
    #   "enddate": "2010-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2010-01-01",
    #   "enddate": "2010-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2009-07-01",
    #   "enddate": "2009-12-31",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
    # {
    #   "startdate": "2009-01-01",
    #   "enddate": "2009-06-30",
    #   "scanner": "PushPercScanner",
    #   "folder": "eod"
    # },
]


client = boto3.client('lambda',
                      aws_access_key_id="AKIAW45OSKUC4RVJA5X3",
                      aws_secret_access_key="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",
                      region_name="us-east-2"
                      )

# input_for_invoker = {
#     'startdate': "2022-01-01",
#     'enddate': '2022-06-30',
#     'scanner': "PushPercScanner",
#     'folder': "eod3",
# }

for input_for_invoker in inputs:
    input_for_invoker["scanner"] = "PushPercScanner"
    input_for_invoker["folder"] = "eo11_perc_push_38"

    response = client.invoke(
        FunctionName='arn:aws:lambda:us-east-2:474423121157:function:lambda_chunkdates',
        InvocationType='Event',
        Payload=json.dumps(input_for_invoker)
    )
    print(f"sent {input_for_invoker}")
    pytime.sleep(60*7)

# "Done Scanning ... "