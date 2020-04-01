# -*- coding: utf-8 -*-

import logging
from math import ceil
import snowflake.connector
from airflow.models import Variable


class TealiumStatusData(object):

    @staticmethod
    def tealium_snowflake_to_s3(file_prefix_, **kwargs):
        """
        Grabs data from a dbt model in Snowflake and posts csv file(s)
        to our internal S3 bucket with the following naming convention:

        [prefix_name]_[version]-[file no.].csv

        ex. "customerstatuses_20190503_12"

        Each file must be no more than 100,000 records (header excluded).
        If over than 100K, multiple files must be created with same
        prefix/version but different file no. (-a,-b,-c, etc.).

        Each time this runs successfully, the max timestamp for the
        customer status data is updated in Airflow variables. On the next run,
        that value is retrieved and is the starting point for the next
        batch of data.

        """

        db_table = "analytics.analytics.tealium_statuses"

        exec_date = kwargs["execution_date"]

        file_version = exec_date.strftime('%Y%m%d_%H-%M-%S')

        file_limit = 100000  # max allowed records per file
        last_timestamp = Variable.get("tealium_etl_last_timestamp")

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

        logging.info("Checking record count in Snowflake.")

        query_chk_data = """

            select
                count(*) as ttl_records,
                date_trunc('second', min(current_date_status)) as min_ts,
                date_trunc('second', max(current_date_status)) as max_ts

            from {0}
            where current_date_status > '{1}'

        """.format(db_table, last_timestamp)

        ttl_records, min_ts, max_ts = cur.execute(query_chk_data).fetchone()

        logging.info(f"{ttl_records} record(s) found.")

        if ttl_records > 0:

            logging.info("Creating Snowflake File Format.")
            cur.execute("""
                        create or replace file format tealium
                        type = csv
                        field_delimiter = ','
                        null_if=('')
                        field_optionally_enclosed_by='"'
                        encoding = UTF8
                        compression = None
                        skip_header=0;
                        """)

            files_needed = ceil(ttl_records / file_limit)
            logging.info(f"{files_needed} file(s) needed.")

            # Beg. of e-mail message to be be sent out via Airflow task
            email_msg = f"<b>Total Records:</b> {ttl_records}<br>" \
                f"<b>Min Record Time: </b>{min_ts} (UTC)<br>" \
                f"<b>Max Record Time: </b>{max_ts} (UTC)<br>" \
                f"<b>Files loaded to S3:</b><br>"

            # For the number of files needed, run query that is offset by the number
            # of rows already pulled in from pevious file, and post to S3
            for i in range(files_needed):

                offset = file_limit * i

                if files_needed == 1:
                    file_name = f"{file_prefix_}_{file_version}.csv"
                else:
                    file_name = f"{file_prefix_}_{file_version}-{chr(i + 97)}.csv"

                logging.info(f"Generating {file_name} - File {i + 1} of {files_needed}.")

                query = """
                    select * from {0}
                    where current_date_status >= '{1}'
                    and current_date_status <= '{2}'
                    order by current_status_id desc
                    limit {3}
                    offset {4}
                    """.format(db_table, min_ts, max_ts, file_limit, offset)

                logging.info("Copying data from Snowflake to S3.")

                cur.execute("""
                    copy into 's3://sdc-marketing-vendors/tealium/{0}'
                    from ({1})
                    credentials = (aws_key_id='{2}' aws_secret_key='{3}')
                    file_format = tealium
                    header = TRUE
                    overwrite = TRUE
                    single = TRUE
                    max_file_size=4900000000;
                    """.format(file_name, query, access_key, secret_key))

                logging.info(f"{file_name} completed and transfered to S3")

                email_msg = email_msg + file_name + '<br>'

            Variable.set("tealium_etl_last_timestamp", max_ts)

        else:

            email_msg = "No new data to send since last run."

        kwargs['ti'].xcom_push(key='etl_results', value=email_msg)
