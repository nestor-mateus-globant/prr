import datetime
import logging
import boto3
import snowflake.connector
from airflow.models import Variable


class HFDDataETL(object):

    @staticmethod
    def hfd_s3_to_snowflake_overwrite(file_name_, table_name_, schema_name_,
                                      database_name_, db_role_, **kwargs):
        """
        Updates HFD data in Snowflake by grabbing data from S3 and overwriting
        existing tables.

        :param file_name_: Name of file in S3 bucket
        :param table_name_: Name of table in Snowflake
        :param schema_name_: Name of schema in Snowflake
        :param database_name_: Name of database in Snowflake
        :param db_role_: Role to use for Snowflake access/writing
        :param kwargs:
        :return: Nothing
        """

        schema = f"{database_name_}.{schema_name_}"

        snowflake_user = Variable.get("snowflake_user")
        snowflake_pwd = Variable.get("snowflake_password")
        snowflake_account = Variable.get("snowflake_account")
        snowflake_role = f'"{db_role_}"'

        aws_access_key = Variable.get("aws_authorization")
        aws_secret_key = Variable.get("aws_secret_key")

        # TODO: Switch this to our new Snowflake DB connector
        logging.info("Connecting to Snowflake.")
        snowflake_cnx = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_pwd,
            account=snowflake_account,
            warehouse='AIRFLOW_WAREHOUSE',
            database=database_name_,
            schema=schema_name_,
            role=snowflake_role
        )

        cur = snowflake_cnx.cursor()

        logging.info("Connecting to S3.")
        s3 = boto3.client('s3', region_name='us-east-2',
                          aws_access_key_id=aws_access_key,
                          aws_secret_access_key=aws_secret_key)

        logging.info("Grabbing S3 object metadata.")
        response = s3.head_object(Bucket='sdc-hfd', Key='sdc-hfd/{}.csv'
                                  .format(file_name_))
        last_modified_ts = response['LastModified']

        logging.info("Running SQL DDL commands.")

        cur.execute("begin")

        try:
            cur.execute("alter table {}.{} drop column processed_time"
                        .format(schema, table_name_))

        except snowflake.connector.errors.ProgrammingError:
            logging.info("\"processed_time\" column already dropped.")

        cur.execute("truncate {}.{}".format(schema, table_name_))

        cur.execute("copy into {0}.{1} from 's3://sdc-hfd/sdc-hfd/{2}.csv' \
                    CREDENTIALS = (AWS_KEY_ID = '{3}' AWS_SECRET_KEY = '{4}') ON_ERROR='continue' \
                    FILE_FORMAT=(skip_header=1,NULL_IF = ('NULL'), FIELD_DELIMITER=',', ESCAPE=NONE) \
                    ".format(schema, table_name_, file_name_,
                             aws_access_key, aws_secret_key))
        cur.execute("alter table {}.{} add column processed_time TIMESTAMP_LTZ"
                    .format(schema, table_name_))
        cur.execute("update {}.{} set processed_time = '{}'"
                    .format(schema, table_name_, str(last_modified_ts)))
        cur.execute("commit")

        logging.info("Complete!")

        # Generating xcom variable for for Airflow e-mail alert
        last_modified_date = last_modified_ts.strftime("%Y-%m-%d")
        color = 'red' if last_modified_date < datetime.datetime.now()\
            .strftime('%Y-%m-%d') else 'green'
        msg = f"{table_name_}: <font color=\"{color}\">"\
              f"{last_modified_date}</font><br>"
        kwargs['ti'].xcom_push(key='etl_results', value=msg)
