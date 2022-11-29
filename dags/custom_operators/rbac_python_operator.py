import boto3
from airflow.operators.python import PythonOperator


STS_TOKEN_VALIDITY = 3600


class RBACPythonOperator(PythonOperator):
    """
    Operator creates a PythonOperator with role bases access control and returns the RBACPythonOperator
    :param / :type python_callable: Inherited, refer Airflow's PythonOperator Class
    :param / :type op_args: Inherited, refer Airflow's PythonOperator Class
    :param / :type op_kwargs: Inherited, efer Airflow's PythonOperator Class
    :param / :type provide_context: Inherited, refer Airflow's PythonOperator Class
    :param / :type templates_dict: Inherited, refer Airflow's PythonOperator Class
    :param / :type templates_exts: Inherited, refer Airflow's PythonOperator Class
    :param task_iam_role_arn: IAM role arn that will be associated during task execution
    :type task_iam_role_arn: str
    """

    def __init__(
        self,
        task_iam_role_arn,
        python_callable,
        op_args=None,
        op_kwargs=None,
        provide_context=False,
        templates_dict=None,
        templates_exts=None,
        **kwargs
    ):
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            provide_context=provide_context,
            **kwargs
        )
        self.task_iam_role_arn = task_iam_role_arn

    def execute(self, context):
        """Airflow PythonOperator Execute Method"""
        assumed_role_object = boto3.client(
            "sts", region_name="ca-central-1"
        ).assume_role(
            RoleArn=self.task_iam_role_arn,
            RoleSessionName="AssumeRoleSession",
            DurationSeconds=STS_TOKEN_VALIDITY,
        )
        task_session = boto3.session.Session(
            aws_access_key_id=assumed_role_object["Credentials"]["AccessKeyId"],
            aws_secret_access_key=assumed_role_object["Credentials"]["SecretAccessKey"],
            aws_session_token=assumed_role_object["Credentials"]["SessionToken"],
        )
        self.op_kwargs["task_session"] = task_session
        super(RBACPythonOperator, self).execute(context)
