from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pendulum


default_args = {
    'start_date': pendulum.today("UTC").add(days=-1),
}

with DAG(
    'write_to_bigquer',
    default_args=default_args,
    schedule_interval="@daily",
    params={"date": "20201101"}
) as dag:

    sql = """
        SELECT
        user_pseudo_id,
        count(*) as number_of_events,
        max(device.mobile_model_name) as device,
        max(device.operating_system) as OS,
        max(geo.country) as country,
        sum(user_ltv.revenue) as revenue
        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        WHERE _TABLE_SUFFIX='{{ params.date }}'
        group by user_pseudo_id
        ORDER BY revenue desc
        """

    bq_write_task = BigQueryInsertJobOperator(
        task_id="write_to_bigquer",
        configuration={
            'query': {
                'query': sql,
                'destinationTable': {
                    'projectId': 'data-analytic-project-405114',
                    'datasetId': 'G4_daily_user',
                    'tableId': 'G4_daily_user_{{ params.date }}'
                },
                'writeDisposition': 'WRITE_TRUNCATE',
                'createDisposition': 'CREATE_IF_NEEDED',
                'useLegacySql': False
            }
        },
        gcp_conn_id='my_gcp_conn',
        dag=dag,
    )
