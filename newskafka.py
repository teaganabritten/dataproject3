import os
import requests
import logging
import json
import sys
from pprint import pformat
from tornado.websocket import websocket_connect
from tornado.ioloop import IOLoop
from tornado import gen
from quixstreams import Application
from prefect import flow, task, get_run_logger
import duckdb

api_key = os.getenv("NEWS_API_KEY")

@task
def getarticles():
    