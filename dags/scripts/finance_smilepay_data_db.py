
from datetime import datetime
import logging
from numpy import rate, ipmt
import pandas as pd
import sqlalchemy as sa
from snowflake.sqlalchemy import URL
from airflow.models import Variable


class FinanceSmilepay(object):

    @staticmethod
    def smilepay_interest_schedules(**kwargs):
        '''
        This calculates missing SmilePay Interest Schedules given inputs pulled
        from Snowflake. Results are used by Finance to determine appropriate
        interest rev. recognition. A certain amount of interest is recognized
        each month over the course of the payment plan.
        '''

        snowflake_user = Variable.get("snowflake_user")
        snowflake_pwd = Variable.get("snowflake_password")
        snowflake_account = Variable.get("snowflake_account")

        logging.info("Connecting to Snowflake.")
        snowflake_cnx = sa.create_engine(URL(
            account=snowflake_account,
            user=snowflake_user,
            password=snowflake_pwd,
            warehouse='AIRFLOW_WAREHOUSE',
            database='RAW',
            schema='AIRFLOW_FINANCE',
            role='AIRFLOW_SERVICE_ROLE'
            ))

        logging.info("Grabbing missing interest schedules from Snowflake.")

        query = """

            select

                fct_aligner_subscription.original_total_installments,
                fct_aligner_subscription.original_amount_financed,
                fct_aligner_subscription.recalc_recurring_amt,
                fct_order_value.net_smilepay_interest

            from analytics.analytics.fct_order_items
            left join analytics.analytics.fct_order_value
                on fct_order_items.item_id = fct_order_value.item_id
            left join analytics.analytics.fct_aligner_subscription
                on fct_order_items.order_id = fct_aligner_subscription.order_id
            left join analytics.analytics.rpt_aligner_revenue_recognition
                on fct_order_items.item_id =
                rpt_aligner_revenue_recognition.item_id

            where fct_order_items.payment_plan = 'SMILEPAY'
            and fct_order_items.date_order >=
                convert_timezone('US/Central', 'UTC',
                cast(to_timestamp('2017-09-01') AS timestamp_ntz))
            and fct_order_items.product_category = 'ALIGNER'
            and rpt_aligner_revenue_recognition.smilepay_apr is null
            and fct_order_value.net_smilepay_interest >= 0
            and fct_aligner_subscription.original_amount_financed > 16
            group by 1, 2, 3, 4

        """

        df_raw = pd.read_sql(query, snowflake_cnx)

        # Ensure df column names are upper case (pandas is case sensitive here)
        df_raw.columns = map(str.upper, df_raw.columns)

        logging.info(f"Calculating {len(df_raw)} interest schedule(s).")

        # Create dataframe to hold calculated schedules
        df_xf = pd.DataFrame(columns=[
            'DATE_ADDED',
            'ORIGINAL_TOTAL_INSTALLMENTS',
            'ORIGINAL_AMOUNT_FINANCED',
            'RECALC_RECURRING_AMT',
            'NET_SMILEPAY_INTEREST',
            'SMILEPAY_APR',
            'INTEREST_MO1',
            'INTEREST_MO2',
            'INTEREST_MO3',
            'INTEREST_MO4',
            'INTEREST_MO5',
            'INTEREST_MO6',
            'INTEREST_MO7',
            'INTEREST_MO8',
            'INTEREST_MO9',
            'INTEREST_MO10',
            'INTEREST_MO11',
            'INTEREST_MO12',
            'INTEREST_MO13',
            'INTEREST_MO14',
            'INTEREST_MO15',
            'INTEREST_MO16',
            'INTEREST_MO17',
            'INTEREST_MO18',
            'INTEREST_MO19',
            'INTEREST_MO20',
            'INTEREST_MO21',
            'INTEREST_MO22',
            'INTEREST_MO23',
            'INTEREST_MO24',
            'INTEREST_MO25',
            'INTEREST_CHECK'
            ])

        record_errors = 0

        # Calculate interest schedules by iterating over rows of df_raw
        for index, row in df_raw.iterrows():

            try:

                # For each calculation, create a dictionary record and add the
                # values to it as calculate (mimicking the column structure of
                # df_xf). Record will then be added to df_xf
                record = {}

                total_installments = row['ORIGINAL_TOTAL_INSTALLMENTS']
                # monthly_payment should be a negative number for rate calc
                monthly_payment = row['RECALC_RECURRING_AMT'] * -1
                total_amount = row['ORIGINAL_AMOUNT_FINANCED']
                total_interest = row['NET_SMILEPAY_INTEREST']

                # Calculate SmilePay APR
                apr = round(
                    rate(nper=total_installments,
                         pmt=monthly_payment,
                         pv=total_amount,
                         fv=0.0,
                         guess=1) * 12
                    , 4)

                record.update({
                    # Add datetime so end user can review recent schedules
                    'DATE_ADDED': datetime.now(),
                    'ORIGINAL_TOTAL_INSTALLMENTS': total_installments,
                    'ORIGINAL_AMOUNT_FINANCED': total_amount,
                    'RECALC_RECURRING_AMT': monthly_payment * -1,
                    'NET_SMILEPAY_INTEREST': total_interest,
                    'SMILEPAY_APR': apr
                    })

                # accum_interest will serve as a check on how much recognized
                # interest we generate in the schedule calculation vs. what
                # the total interest on the order is
                accum_interest = 0.0

                # Plan's can have a max of 25 installments. We will iterate
                # through 25 months here so as to account for all columns
                # in df_xf (even if the plan is less than 25 installments)
                for i in range(1, 26):

                    col = f'INTEREST_MO{i}'

                    if i == total_installments:
                        # If this is the last installment in the plan, then
                        # interest should be the difference between total beg.
                        # interest and total calculated so far (rem. amount)
                        interest_due = total_interest - accum_interest
                    elif i < total_installments:
                        # If this installment is less than total installments,
                        # calculate interest due as per normal.
                        interest_due = ipmt(rate=apr/12,
                                            per=i,
                                            nper=total_installments,
                                            pv=total_amount) * -1
                    else:
                        # Return 0 if beyond the total num of plan installments
                        interest_due = 0.0

                    interest_due = round(interest_due, 2)
                    accum_interest += interest_due
                    record.update({col: interest_due})

                record.update({"INTEREST_CHECK": accum_interest})

                df_xf = df_xf.append(record, ignore_index=True)

            except Exception as e:
                record_errors += 1
                logging.exception(f"Issue calculating schedule w/ "
                                  f"installments = {total_installments}, "
                                  f"payment = {monthly_payment}, "
                                  f"amount = {total_amount}, "
                                  f"and interest = {total_interest}")

        df_xf.to_sql("smilepay_interest_schedule",
                     con=snowflake_cnx,
                     schema="airflow_finance",
                     if_exists="append",
                     index_label=None,
                     index=False)

        msg = f"<b>SmilePay Interest Schedules: </b>"\
            f"{len(df_xf)} new schedule(s) added. "\
            f"{record_errors} schedule "\
            f"calculations errored out."

        kwargs['ti'].xcom_push(key='etl_results', value=msg)
