# -*- coding: utf-8 -*-

""" This module exports MTA results to gpg file and directly on FTP site. """

import sys
import os
from datetime import datetime, timedelta, tzinfo
import gnupg
import paramiko
import pandas as pd
import logging
from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
from sdc_etl_libs.database_helpers.SnowflakeDatabase import SnowflakeDatabase
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers

class NeustarMtaData(object):

    @staticmethod
    def neustar_mta_to_ftp(**kwargs):
        now = datetime.utcnow()
        today = datetime(now.year, now.month, now.day)
        yesterday = today + timedelta(days=-1)

        fdate = yesterday
        from_date = "%04d-%02d-%02d" % ( fdate.year, fdate.month, fdate.day )

        tdate = fdate + timedelta(days=1)
        to_date = "%04d-%02d-%02d" % ( tdate.year, tdate.month, tdate.day )

        db_creds = AWSHelpers.get_secrets(
            "snowflake/service_account/airflow")

        snowflake_user = db_creds["username"]
        snowflake_pwd = db_creds["password"]
        snowflake_account = db_creds["account"]

        sftp_creds = AWSHelpers.get_secrets(
            "neustar-onboarding/sftp")

        sftp_host = sftp_creds["host"]
        sftp_password = sftp_creds["password"]
        sftp_port = sftp_creds["port"]
        sftp_user = sftp_creds["user"]

        encrypt_data = AWSHelpers.get_secrets(
            "neustar-onboarding/pgp-public")

        neustar_public_key = encrypt_data["public-pgp-key"].encode().decode('unicode_escape')

        logging.info("Connecting to Snowflake...")

        dbh = SnowflakeDatabase(sqlalchemy_=True)
        dbh.connect(
           warehouse_='AIRFLOW_WAREHOUSE',
           database_='RAW',
           schema_='FIVETRAN_TESSERACT_PUBLIC',
           role_='AIRFLOW_SERVICE_ROLE',
           user_=snowflake_user,
           password_=snowflake_pwd,
           account_=snowflake_account,
           airflow_=True)
        snowflake_cnx = dbh.connection

        logging.info("Connected to Snowflake.")

        query_file = SDCFileHelpers.get_file_path("sql","Neustar/onboarding_query.sql")
        with open(query_file,"r") as qf: query = qf.read()
        qf.close()
        query = query % (from_date, to_date)

        logging.info("MTA Query beginning.")

        df = pd.read_sql(query, snowflake_cnx)
        curs = df.to_dict('records')
        tabdelim = "\t".join(['CUSTID', 'STREET', 'SECONDARY', 'CITY', 'STATE', 'POSTAL', 'PHONE', 'EMAIL', 'TRANSACTION_DATE', 'TRANSACTION_TYPE', 'TRANSACTION_VALUE', 'REFERRAL', 'LOCATION', 'PAYMENT', 'FINANCED', 'DISCOUNT_AMOUNT', 'DISCOUNT_CODE', 'UTM_SOURCE', 'HEAP_USER_ID']) + "\n"
        cnt = 0
        for r in curs:
            r2 = [r['custid'], r['street'], r['secondary'], r['city'], r['state'], r['postal'], r['phone'], r['email'], r['transaction_date'], r['transaction_type'], r['transaction_value'], r['referral'], r['location'], r['payment'], r['financed'], r['discount_amount'], r['discount_code'], r['utm_source'], r['heap_user_id']]
            tabdelim += "\t".join([str(elem) for elem in r2]) + "\n"
            cnt = cnt + 1

        email_msg = f"<b>Total Records: {cnt}</b><br>"

        if (os.path.exists('/tmp/mtagpg')):
            os.system('rm -rf /tmp/mtagpg')
        gpg = gnupg.GPG( homedir="/tmp/mtagpg", binary='/usr/bin/gpg' )

        import_result = gpg.import_keys(neustar_public_key)
        fingerprint = ''
        if len(import_result.fingerprints) > 0:
            fingerprint = import_result.fingerprints[0]
        if fingerprint != '':
            encrypted_data = gpg.encrypt(tabdelim, fingerprint)
            encrypted_string = str(encrypted_data)

            if encrypted_data.ok:
                remote_file = "SmileDirectClub-transactions-%04d%02d%02d.txt.gpg" % (fdate.year, fdate.month, fdate.day)
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(sftp_host, username = sftp_user, password = sftp_password)
        
                logging.info("FTP Connection opened.")
                ftp = ssh.open_sftp()
                file = ftp.file(remote_file, "w", -1)
                file.write(encrypted_string)
                file.flush()
                file.close()
                stats = ftp.stat(remote_file)
                email_msg = f"{email_msg}<br><b>File Sent to FTP:</b><br>"\
                    f"{remote_file} ({stats.st_size} Bytes)"
                ftp.close()
                ssh.close()
                logging.info("FTP Connection closed.")
            else:
                email_msg = f"{email_msg}<br><b>Error encrypting MTA data: {encrypted_data.status}: {encrypted_data.stderr}"
        else:
            email_msg = f"{email_msg}<br><b>Error generating encryption key"


        kwargs['ti'].xcom_push(key='etl_results', value=email_msg)

