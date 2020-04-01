

import datetime
import logging
import os
import time
import numpy as np
import pandas as pd
import stripe
from airflow.models import Variable
from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
from sdc_etl_libs.database_helpers.DatabaseFactory import DatabaseFactory


class Stripe:

    @staticmethod
    def get_payment_method_details(api_secrets_: str, table_name_: str,
                                   db_role_: str, **kwargs):
        """
        Grabs Payment Method Detail information from the
        Stripe Charge API and writes to Snowflake.

        :param api_secrets_: AWS Secrets name for API key
        :param table_name_: Database table name to write to
        :param db_role_: Database role to use
        :return: None
        """

        database = "RAW"
        schema = "STRIPE"

        # May or may not need keys depending on depending on
        # environment's AWS permissions
        aws_access_key = Variable.get("aws_authorization")
        aws_secret_key = Variable.get("aws_secret_key")

        exec_date = kwargs["execution_date"]
        end_date = (exec_date + datetime.timedelta(days=1))
        start_date = (exec_date - datetime.timedelta(days=2))
        logging.info(f"Running with start_date of {start_date} and "
                     f"end_date of {end_date}.")

        api_creds = AWSHelpers.get_secrets(
            api_secrets_,
            access_key_=aws_access_key,
            secret_key_=aws_secret_key
        )

        db_creds = AWSHelpers.get_secrets(
            "snowflake/service_account/airflow",
            access_key_=aws_access_key,
            secret_key_=aws_secret_key
        )

        db_handle = DatabaseFactory.get_database(
            "snowflake", sqlalchemy_=True)

        db_handle.connect(
            user_=db_creds["username"],
            password_=db_creds["password"],
            account_=db_creds["account"],
            warehouse_=db_creds["warehouse"],
            database_=database,
            schema_=schema,
            role_=db_role_,
            airflow_=False
        )

        stripe.api_key = api_creds["key"]

        # Unix time is used in API call. Changing start/end date
        # to Unix time here
        gte = int((time.mktime(start_date.timetuple())))
        lt = int((time.mktime(end_date.timetuple())))

        # API limit on Charges is 100 per call.
        try:
            charges = stripe.Charge.list(limit=100, created={'gte': gte, 'lt': lt})
        except Exception as e:
            logging.info(e)

        df = pd.DataFrame(columns={
            "CHARGE_ID",
            "CREATED",
            "BRAND",
            "COUNTRY",
            "DESCRIPTION",
            "EXP_MONTH",
            "EXP_YEAR",
            "FINGERPRINT",
            "FUNDING",
            "IIN",
            "ISSUER",
            "LAST4",
            "ADDRESS_LINE1_CHECK",
            "ADDRESS_POSTAL_CODE_CHECK",
            "CVC_CHECK"
        })

        logging.info("Grabbing data from API and adding to data frame...")
        for charge in charges.auto_paging_iter():

            df = df.append({
                "CHARGE_ID": charge["id"],
                "CREATED": charge["created"],
                "BRAND": charge["payment_method_details"]["card"]["brand"],
                "COUNTRY": charge["payment_method_details"]["card"]["country"],
                "EXP_MONTH": charge["payment_method_details"]["card"]["exp_month"],
                "EXP_YEAR": charge["payment_method_details"]["card"]["exp_year"],
                "FINGERPRINT": charge["payment_method_details"]["card"]["fingerprint"],
                "FUNDING": charge["payment_method_details"]["card"]["funding"],
                "LAST4": charge["payment_method_details"]["card"]["last4"],
                "ADDRESS_LINE1_CHECK": charge["payment_method_details"]["card"]["checks"]["address_line1_check"],
                "ADDRESS_POSTAL_CODE_CHECK":
                    charge["payment_method_details"]["card"]["checks"]["address_postal_code_check"],
                "CVC_CHECK": charge["payment_method_details"]["card"]["checks"]["cvc_check"],
                # These next three elements are the ones Stripe is exposing in the API for us. They may or may not
                # exist in charge events prior to go-live, therefore, we check here to see if element exists,
                # and if not, return None
                "DESCRIPTION": charge.get('payment_method_details', {}).get('card', {}).get('description', None),
                "IIN": charge.get('payment_method_details', {}).get('card', {}).get('iin', None),
                "ISSUER": charge.get('payment_method_details', {}).get('card', {}).get('issuer', None)
            }, ignore_index=True
        )

        logging.info("Cleaning up data frame.")
        # Replace all blanks with NaN - will become NULL in database
        df.replace('', np.nan, inplace=True)

        # Convert Unix timestamps to ISO format
        df['CREATED'] = pd.to_datetime(df['CREATED'], unit='s')

        logging.info("Writing data to temp table in Snowflake.")

        db_handle.execute_query(f"create or replace table {database}.{schema}.{table_name_}_temp "
                                f"as select * from {database}.{schema}.{table_name_} "
                                f"where 1 = 0")

        df.to_sql(f"{table_name_}_temp",
                  con=db_handle.connection,
                  schema=schema,
                  if_exists="append",
                  index_label=None,
                  index=False,
                  chunksize=15000  # Breaks up insert statements
                  )

        with open(os.path.dirname(os.path.abspath(__file__)) +
                  "/sql/MERGE_INTO_payment_method_details_XXX_pii.sql") as f:
            merge_query = f.read().format(database, schema, table_name_)

        logging.info("Merging temp table into main table.")
        db_handle.execute_query(merge_query, return_results_=True)

        # Generate xcom for Airflow notification e-mail
        for row in db_handle.get_results():
            inserted = row[0]
            updated = row[1]

        color = 'red' if (inserted == 0 and updated == 0) else 'green'
        msg = f"{table_name_}: <font color=\"{color}\">" \
            f"{inserted:,} rows inserted. " \
            f"{updated:,} rows updated.</font><br>"

        logging.info(msg)
        kwargs['ti'].xcom_push(key='etl_results', value=msg)

        logging.info("Dropping temp table")
        db_handle.execute_query(f"drop table {database}.{schema}.{table_name_}_temp")
