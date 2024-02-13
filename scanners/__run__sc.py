import json
from scanners.main_sc import run

# def lambda_handler(event, context):  # extract values from this ...


def lambda_handler(event, context):
    run()

lambda_handler( "", "")