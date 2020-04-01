# -*- coding: utf-8 -*-

""" This module exports web data to zip file and sends to FTP site. """

import datetime
import logging
import os
import zipfile
import pandas as pd
import paramiko
import snowflake.connector
from airflow.models import Variable
from airflow import AirflowException


class NeustarWebData(object):

    @staticmethod
    def neustar_snowflake_to_ftp(**kwargs):
        start_date = kwargs["execution_date"]
        end_date = (start_date + datetime.timedelta(days=1))\
            .strftime('%Y-%m-%d')

        snowflake_user = Variable.get("snowflake_user")
        snowflake_pwd = Variable.get("snowflake_password")
        snowflake_account = Variable.get("snowflake_account")

        ftp_host = Variable.get("neustar_ftp_mdc_hostname")
        ftp_password = Variable.get("neustar_ftp_mdc_password")
        ftp_port = int(Variable.get("neustar_ftp_mdc_port"))
        ftp_user = Variable.get("neustar_ftp_mdc_user")

        logging.info("Connecting to Snowflake...")
        snowflake_cnx = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_pwd,
            account=snowflake_account,
            warehouse='AIRFLOW_WAREHOUSE',
            database='ANALYTICS',
            schema='ANALYTICS',
            role='AIRFLOW_SERVICE_ROLE'
        )
        logging.info("Connected to Snowflake.")

        logging.info("Connecting to FTP...")

        transport = paramiko.Transport((ftp_host, ftp_port))
        transport.connect(username=ftp_user, password=ftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        logging.info("Connected to FTP.")

        zip_file_name = start_date.strftime('%Y-%m-%d') + "_HeapAnalytics.zip"
        zip_file = zipfile.ZipFile(zip_file_name, "w")

        logging.info("Empty zip file created.")

        queries = {

            'heap_sessions':

                """

                    select
                        event_id,
                        user_id,
                        session_id,
                        "TIME",
                        library,
                        platform,
                        device_type,
                        country,
                        region,
                        city,
                        ip,
                        regexp_replace(
                            lower(referrer),
                            '(utm_term=|email=)[a-za-z0-9._-\w]+[@][a-za-z0-9._-\w]+(&|)','')
                        as referrer,
                        landing_page,
                        browser,
                        search_keyword,
                        utm_source,
                        utm_campaign,
                        utm_medium,
                        utm_content

                    from raw.heap_materialized.sessions

                    where "TIME"::date >= '{0}'
                    and "TIME"::date < '{1}'
                    and ip not in (
                        select ip from
                        analytics.analytics.data_cleanup_remove_traffic
                        )

                """,

            'heap_events': """

                    select
                        user_id,
                        "TIME",
                        event_id,
                        session_id,
                        event_table_name

                    from raw.heap_materialized.all_events

                    where "TIME"::date >= '{0}'
                    and "TIME"::date < '{1}'
                    and session_id not in (
                        select session_id
                        from analytics.analytics.data_cleanup_remove_traffic
                        )

                """,

            'heap_users': """

                    select
                        from_user_id,
                        to_user_id,
                        null as "TIME"

                    from raw.heap_materialized.user_migrations order by to_user_id desc
                    limit 15000

                    --where "TIME"::date >= '{0}'
                    --and "TIME"::date < '{1}'
                    --and from_user_id not in (
                    --   select user_id
                    --    from analytics.analytics.data_cleanup_remove_traffic
                    --    )
                    --and to_user_id not in (
                    --   select user_id from
                    --    analytics.analytics.data_cleanup_remove_traffic
                    --    )

                """

            }

        logging.info("Query runs beginning.")

        email_msg = "<b>Total Records:</b><br>"

        # TODO: Figure out a way to do this memory.
        for key, value in queries.items():

            file_name = "SmileDirectClub-" + \
                    key + "_" + start_date.strftime('%Y-%m-%d') + ".txt"

            query = value.format(start_date, end_date)
            df = pd.read_sql(query, snowflake_cnx)

            # Error out if any of the tables are empty
            if key != 'heap_users' and len(df) == 0:
                raise AirflowException(f"Incomplete data. "
                                       f"Querying {key} returned "
                                       f"{len(df):,} records.")

            logging.info("{0} query saved to df.".format(key))

            df.to_csv(file_name, index=None, sep='|')

            # Attempt to free up memory
            del df

            with open(file_name, encoding="utf8") as fname:
                for idx, lines in enumerate(fname):
                    pass
                records = idx + 1

            logging.info(f"{key} df saved to drive: {records} lines.")
            email_msg = f"{email_msg}{key}: {records:,}<br>"

            zip_file.write(file_name,
                           os.path.basename(file_name),
                           zipfile.ZIP_DEFLATED)

            logging.info("{0} saved to zip.".format(key))

            os.remove(file_name)

            logging.info("{0} file removed from disk.".format(key))

        zip_file.close()

        sftp.put(zip_file_name, '/Incoming/' + os.path.basename(zip_file_name))
        stats = sftp.stat('/Incoming/' + os.path.basename(zip_file_name))
        logging.info(f"Confirmed posting to FTP:\n{zip_file_name}.")
        email_msg = f"{email_msg}<br><b>File(s) sent to FTP:</b><br>"\
            f"{zip_file_name} ({round(stats.st_size / 1000000, 2)} MB)"

        transport.close()

        logging.info("FTP Connection closed.")

        os.remove(zip_file_name)

        logging.info("Zip file removed from disk.")

        kwargs['ti'].xcom_push(key='etl_results', value=email_msg)
