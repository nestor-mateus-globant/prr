# -*- coding: utf-8 -*-

""" This module exports web data an SDC S3 bucket for Horizon's use. """

import datetime
import logging
import snowflake.connector
from airflow.models import Variable


class HorizonWebData(object):

    @staticmethod
    def horizon_snowflake_to_s3(seasoning_=90, **kwargs):

        exec_date = kwargs["execution_date"]
        end_date = (exec_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (exec_date - datetime.timedelta(days=seasoning_)).strftime('%Y-%m-%d')

        snowflake_user = Variable.get("snowflake_user")
        snowflake_pwd = Variable.get("snowflake_password")
        snowflake_account = Variable.get("snowflake_account")

        access_key = Variable.get("aws_authorization_marketing_vendors")
        secret_key = Variable.get("aws_secret_key_marketing_vendors")

        logging.info("Connecting to Snowflake.")
        snowflake_cnx = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_pwd,
            account=snowflake_account,
            warehouse='AIRFLOW_WAREHOUSE',
            database='ANALYTICS',
            schema='ANALYTICS',
            role='AIRFLOW_SERVICE_ROLE'
        )

        cur = snowflake_cnx.cursor()

        logging.info("Creating Snowflake File Format.")
        cur.execute("""
              create or replace file format horizon_next 
              type = csv field_delimiter = ',' 
              null_if=('') 
              field_optionally_enclosed_by='"' 
              compression = gzip 
              encoding = UTF8 
              skip_header=0;
              """)

        logging.info("Creating Snowflake Stage.")
        cur.execute("""
              create or replace stage horizon_next 
              file_format = horizon_next 
              URL = 's3://sdc-marketing-vendors/horizon-next' 
              credentials = (aws_key_id = '{0}' aws_secret_key = '{1}');
              """.format(access_key, secret_key))

        logging.info("Copying data from Snowflake to S3.")
        cur.execute("""
              copy into @horizon_next/web_data/web_data_{0}_to_{2}_{3}days.csv.gz
              from (select * from analytics.r2c_export where "TIME"::date >= '{0}' and "TIME"::date < '{1}') 
              file_format = horizon_next 
              header = TRUE 
              overwrite = TRUE
              single = TRUE
              max_file_size=4900000000;
              """.format(start_date, end_date, exec_date.strftime('%Y-%m-%d'), str(seasoning_)))

        logging.info("Complete!")
