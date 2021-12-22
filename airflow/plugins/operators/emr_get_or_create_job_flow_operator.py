import logging

from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.utils import apply_defaults


class EmrGetOrCreateJobFlowOperator(EmrCreateJobFlowOperator):
    """
        Tries to find EMR cluster by name or creates it if not found
        :param aws_conn_id  – aws connection to uses
        :type aws_conn_id: str
        :param emr_conn_id  – emr connection to use
        :param job_flow_overrides – boto3 style arguments to override emr_connection extra. (templated)
        :param job_flow_overrides: dict
    """
    @apply_defaults
    def __init__(
            self,
            aws_conn_id='s3_default',
            emr_conn_id='emr_default',
            job_flow_overrides=None,
            region_name=None,
            *args, **kwargs):
        super(EmrCreateJobFlowOperator, self).__init__(*args, **kwargs)
        self.job_flow_overrides = job_flow_overrides
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        self.region_name = region_name

    def execute(self, context):
        emr = EmrHook(aws_conn_id=self.aws_conn_id,
                      emr_conn_id=self.emr_conn_id,
                      region_name=self.region_name)
        cluster_name = self.job_flow_overrides['Name']
        logging.info(f"Cluster name {cluster_name}")
        if cluster_name is not None:
            cluster_id = emr.get_cluster_id_by_name(cluster_name, ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
            if cluster_id is not None:
                return cluster_id
            else:
                return super().execute(context)
        else:
            return super().execute(context)
