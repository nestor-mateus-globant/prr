# -*- coding: utf-8 -*-

""" This module exports e-mail data to delim. text file and sends to FTP site. """

import datetime
import os
import gzip
import logging
import paramiko
import snowflake.connector
from airflow.models import Variable
from airflow import AirflowException

#TODO: this does not scale. Make it scale.

class NeustarEmailData(object):

    @staticmethod
    def neustar_snowflake_to_ftp(**kwargs):

        start_date = kwargs["execution_date"]
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = (start_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        snowflake_user = Variable.get("snowflake_user")
        snowfake_pwd = Variable.get("snowflake_password")
        snowflake_account = Variable.get("snowflake_account")

        ftp_hostname = Variable.get("neustar_ftp_onboarding_hostname")
        ftp_password = Variable.get("neustar_ftp_onboarding_password")
        ftp_port = int(Variable.get("neustar_ftp_onboarding_port"))
        ftp_user = Variable.get("neustar_ftp_onboarding_user")

        logging.info("Connecting to Snowflake...")
        snowflake_cnx = snowflake.connector.connect(
            user=snowflake_user,
            password=snowfake_pwd,
            account=snowflake_account,
            warehouse='AIRFLOW_WAREHOUSE',
            database='ANALYTICS',
            schema='ANALYTICS',
            role='AIRFLOW_SERVICE_ROLE'
        )

        cur = snowflake_cnx.cursor()

        logging.info("Connected to Snowflake!")

        query_chk_data = """

            select 
                count(*) as ttl_records, 
                date_trunc('minute', min(event_timestamp)) as min_ts, 
                date_trunc('minute', max(event_timestamp)) as max_ts

            from analytics.exp_neustar_email_events
            where event_timestamp::date >= '{0}'
            and event_timestamp::date < '{1}'

        """.format(start_date_str, end_date_str)

        ttl_records, min_ts, max_ts = cur.execute(query_chk_data).fetchone()

        logging.info(f"{ttl_records:,} records found.")

        # Sense check data here and stop if data looks incomplete.
        if ttl_records < 1000:

            raise AirflowException(f"Incomplete data - only {ttl_records:,} records found.")

        email_msg = f"<b>Total Records:</b> {ttl_records:,}<br>" \
            f"<b>Min Record Time: </b>{min_ts} (UTC)<br>" \
            f"<b>Max Record Time: </b>{max_ts} (UTC)<br>" \
            f"<b>Files sent to FTP:</b><br>"

        query = """ select
                        	coalesce(email_address,'') as email_address,
                        coalesce(event_type,'') as event_type,
                        event_timestamp as event_timestamp,
                        coalesce(mailing_name,'') as mailing_name,
                        coalesce(mailing_group,'') as mailing_group,
                        -- Need to remove all Emojios and sepecial characters in e-mail subject
                        coalesce(
                                trim(
                                        regexp_replace(mailing_subject, '[^a-zA-Z0-9 .@%,=()?!-:_,\\']+', ' ')
                                        )
                                , '') as mailing_subject,
                        coalesce(transactional,'') as transactional,
                        coalesce(cost_of_send,'') as cost_of_send,
                        coalesce(user_id::text,'') as user_id,
                        coalesce(street1,'') as street1,
                        coalesce(street2,'') as street2,
                        coalesce(city,'') as city,
                        coalesce(state,'') as state,
                        coalesce(zip,'') as zip,
                        coalesce(phone,'') as phone,
                        coalesce(first_lead_id::text,'') as first_lead_id,
                        coalesce(heap_user_id::text,'') as heap_user_id

                    from analytics.exp_neustar_email_events
                    where event_timestamp::date >= '{0}'
                    and event_timestamp::date < '{1}'
 
                """.format(start_date_str, end_date_str)

        logging.info("Query run started.")

        fname = f"SmileDirectClub-email_events-{start_date_str}_to_{start_date_str}.txt.gz"

        with gzip.open(fname, mode='wt', encoding='utf-8') as f:
            logging.info("Running query...")
            headers = cur.execute(query)
            f.write('|'.join([col[0] for col in headers.description]) + "\n")
            results = cur.execute(query).fetchall()
            logging.info("Writing to file...")
            for row in results:
                record = ""
                for col in row:
                    record = record + str(col) + "|" 
                record = record[:-1] + "\n" # Remove unncessary final pipe
                f.write(record)
        logging.info(f"{fname} created and closed.")

        logging.info("Connecting to FTP...")

        try:
            transport = paramiko.Transport((ftp_hostname, ftp_port))
            transport.connect(username=ftp_user, password=ftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            logging.info("Connected to FTP.")

            sftp.put(fname, os.path.basename(fname))
            logging.info(f"{fname} sent to FTP site.")

            stats = sftp.stat(os.path.basename(fname))
            logging.info(f"Confirmed posting to FTP:\n{fname}.")
            email_msg = f"{email_msg} {fname} ({round(stats.st_size/1000000,2)} MB)"

            transport.close()
            logging.info("FTP connection closed.")

        except Exception as e:
            logging.exception("Error connecting/posting to FTP site.")

        finally:
            os.remove(fname)

        kwargs['ti'].xcom_push(key='etl_results', value=email_msg)
