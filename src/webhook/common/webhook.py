#!/usr/bin/python3
#-*-coding:utf-8-*-

import requests
import psycopg2
import psycopg2.extras
import sys
import hashlib
import json

sys.path.append('../../common')
from dataimporters import DataImporter

