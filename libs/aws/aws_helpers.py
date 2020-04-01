import boto3
import logging
import time

logging.basicConfig(level=logging.INFO)


class GlueHelpers(object):
    @staticmethod
    def start_glue_job(job_name_, job_args_, dpu_, region_="us-east-2"):
        """
        :param job_name_: str
        :param job_args_: dict(str:str)
        :param region_: str
        :return: str : JobRunId  or None on failure
        """
        client = boto3.client('glue', region_name=region_)
        response = client.start_job_run(
            JobName=job_name_,
            Arguments=job_args_,
            AllocatedCapacity=dpu_,
            Timeout=200
        )

        print response
        return response["JobRunId"]

    @staticmethod
    def poll_glue_job_run(job_name_, job_run_id_, region_="us-east-2"):
        """
        :param job_name_: str
        :param job_run_id_: str
        :param region_: str
        :return: bool : return true
        raises exception on unknown states
        """

        fail_conditions = ['TIMEOUT', 'FAILED', 'STOPPED']
        continue_conditions = ['STARTING', 'RUNNING', 'STOPPING']
        success_conditions = ['SUCCEEDED']

        client = boto3.client('glue', region_name=region_)
        response = client.get_job_run(
            JobName=job_name_,
            RunId=job_run_id_,
        )
        while True:
            status = response["JobRun"]['JobRunState']
            logging.info("Glue Job Status: %s" % status)
            if status in continue_conditions:
                time.sleep(30)
                response = client.get_job_run(
                    JobName=job_name_,
                    RunId=job_run_id_,
                )
                continue
            elif status in fail_conditions:
                return False
            elif status in success_conditions:
                return True
            else:
                logging.error("Unknown job state.")
                raise Exception("Unknown GLUE job state.")

    @staticmethod
    def stop_glue_job(job_name_, job_run_id_, region_="us-east-2"):
        """
        :param job_name_: str
        :param job_run_id_: str
        :param region_: str
        """
        client = boto3.client('glue', region_name=region_)
        response = client.batch_stop_job_run(
            JobName=job_name_,
            JobRunIds=[
                job_run_id_,
            ]
        )
        logging.info(response)
