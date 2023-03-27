import requests
import json
import requests
import json
import sys
import time
import numpy
import jwt
from pathlib import Path
from datetime import datetime
import pandas as pd
import psycopg2
import pandasql as ps
import warnings
from collections import Counter
import boto3
from botocore.exceptions import ClientError
import os
import logging
import configparser

now = datetime.now()
dt_string = now.strftime("%d-%m-%YZ%H-%M-%S")
logging.basicConfig(
    filename="Query Profile Analysis" + dt_string + ".log",
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)
class AepConfig:
    def __init__(self, apiconfig):
        config = configparser.ConfigParser()
        config.read(apiconfig)
        # print(config.sections())
        self.client_id = config["APICREDENTIALS"]["client_id"]
        self.client_secret = config["APICREDENTIALS"]["client_secret"]
        self.xapikey = config["APICREDENTIALS"]["xapikey"]
        self.xgwimsorgid = config["APICREDENTIALS"]["xgwimsorgid"]
        self.xsandboxname = config["APICREDENTIALS"]["xsandboxname"]
        self.org_id = config["APICREDENTIALS"]["org_id"]
        self.technical_account_id = config["APICREDENTIALS"]["technical_account_id"]
        self.private_key = config["APICREDENTIALS"]["private_key"]
        self.tenant_id = config["APICREDENTIALS"]["tenant_id"]
        self.jwt_token = self.jwtToken()
        self.access_token = self.getAccessToken()
        self.SegmentId=config['APICREDENTIALS']['SegmentId']
        self.Host=config['APICREDENTIALS']['Host']
        self.ProfileSnapshottable=config['APICREDENTIALS']['ProfileSnapshottable']
        self.outputcsv=config['APICREDENTIALS']['outputcsv']

    def jwtToken(self):
        with open(self.private_key, "r") as f:
            private_key_unencrypted = f.read()
            header_jwt = {
                "cache-control": "no-cache",
                "content-type": "application/x-www-form-urlencoded",
            }
        jwtPayload = {
            "exp": round(24 * 60 * 60 + int(time.time())),
            "iss": self.org_id,
            "sub": self.technical_account_id,
            "https://ims-na1.adobelogin.com/s/ent_dataservices_sdk": True,
            "aud": "https://ims-na1.adobelogin.com/c/" + self.xapikey,
        }
        encoded_jwt = jwt.encode(jwtPayload, private_key_unencrypted, algorithm="RS256")
        return encoded_jwt

    def getAccessToken(self):
        url = "https://ims-na1.adobelogin.com/ims/exchange/jwt/"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "jwt_token": self.jwt_token,
        }
        files = []
        headers = {"Cookie": "relay=00c261e1-89fe-4ea4-981f-53f2df749aa9; ftrset=11"}
        response = requests.request(
            "POST", url, headers=headers, data=payload, files=files
        )
        # print(response.text)
        access_token = json.loads(response.text)["access_token"]
        return access_token

class query_final:
        def __init__(self,apiconfig):
             self.config=apiconfig
        def get_qs_conn(self):
            host=self.config.Host
            sandbox_name=self.config.xsandboxname
            token=self.config.access_token
            ims_org=self.config.xgwimsorgid
            qs_conn=psycopg2.connect(
                sslmode='require',
                host=host,
                port=80,
                dbname='%s:all' % sandbox_name,
                user=ims_org,
                password=token,
                application_name='Profile Snapshot'
            )
            return qs_conn
        def query(self,qs_conn):
            snapshot_id_query="""SELECT COALESCE(parent_id, 'HEAD') as snapshotid FROM (SELECT parent_id FROM ( SELECT history_meta('profile_snapshot_export_2da00b78_9365_4b49_be32_5d4248f2d3a7') )WHERE is_current=true)"""
            snapshot_id_df=pd.read_sql_query(snapshot_id_query,qs_conn)
            for i in snapshot_id_df['snapshotid']:
                snapshot_id=i
            logging.info(snapshot_id)
            profile_snapshot_query = """SELECT key as segmentID, value.lastQualificationTime,value.status, identityMap.customerid as upmid
            FROM (SELECT explode(value),identityMap
            FROM (SELECT explode(segmentMembership), identityMap
            FROM {} snapshot since '{}')) WHERE key = '{}' AND value.status != 'exited'
            ;""".format(self.config.ProfileSnapshottable,snapshot_id,self.config.SegmentId)
            total_event_count_df = pd.read_sql_query(profile_snapshot_query, qs_conn)
            Segment_Id_Final=[]
            Last_Qualification_Time=[]
            Status=[]
            Upmid=[]
            for k in total_event_count_df['segmentID']:
                Segment_Id_Final.append(k)

            for a in total_event_count_df['lastqualificationtime']:
                Last_Qualification_Time.append(a)

            for b in total_event_count_df['status']:
                Status.append(b)

            for j in total_event_count_df['upmid']:
                Upmid.append(j)
            Final_UPMIDS=[]
            for h in range(0,len(Upmid)):
                upmids=Upmid[h].split(',')
                Final_UPMIDS.append(upmids[1])

            df=pd.DataFrame({'SegmentId':Segment_Id_Final,'Last Qualification Time':Last_Qualification_Time,'Status':Status,'UPMIDs':Final_UPMIDS})
            df.to_csv(self.config.outputcsv)


class main:
    def __init__(self, apiconfig):
        self.config = AepConfig(apiconfig)
    def output(self):
        query_output=query_final(self.config)
        qs_conn=query_output.get_qs_conn()
        query_output.query(qs_conn)

mainfunction=main("queryconfig.ini")
mainfunction.output()
