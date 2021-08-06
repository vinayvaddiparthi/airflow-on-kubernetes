"""
# Equifax Consumer Outbound DAG

This workflow sends the Equifax consumer request file (i.e. eligible applicant information) to
Equifax on a monthly basis for recertification purposes.
"""

dag = DAG(
    "equifax_consumer_outbound",
    default_args=default_args,
    description="A workflow to send the consumer batch request file to Equifax",
    schedule_interval="@daily",
    start_date=datetime(2021, 8, 15),
    tags=['equifax'],
)
dag.doc_md = __doc__