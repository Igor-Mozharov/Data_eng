import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'formal-grammar-407607-97b29afb27d0.json'

PROJECT = 'formal-grammar-407607'
BUCKET = 't_data_bucket'
INPUT_FILE = 'gs://t_data_bucket/2023-12-13/events.json'.format(BUCKET)
OUTPUT_TABLE = '{}.test_dataset.your_table'.format(PROJECT)

table_schema = {
    'fields': [
        {'name': 'user_params', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
            {'name': 'is_active_user', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'dclid', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'medium', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'marketing_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'term', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'campaign_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'context', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'gclid', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'transaction_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'specification', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'srsltid', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'model', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'model_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'os', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]},
        {'name': 'install_store', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'analytics_storage', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'engagement_time_msec', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'event_origin', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'localization_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'event_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'session_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'event_time', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'user_action_detail', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'model_number', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'place', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'numeric', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'official_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'model', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ga_session_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'current_progress', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'flag', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'alpha_3', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'browser', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'specification', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'selection', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
        {'name': 'alpha_2', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'os', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}


class TransformData(beam.DoFn):
    def process(self, element):
        data = json.loads(element)
        transformed_data = {
            'user_id': data.get('user_id', ''),
            'event_time': data.get('event_time', ''),
            'alpha_2': data.get('alpha_2', ''),
            'alpha_3': data.get('alpha_3', ''),
            'flag': data.get('flag', ''),
            'name': data.get('name', ''),
            'numeric': data.get('numeric', ''),
            'official_name': data.get('official_name', ''),
            'os': data.get('os', ''),
            'brand': data.get('brand', ''),
            'model': data.get('model', ''),
            'model_number': data.get('model_number', 0),
            'specification': data.get('specification', ''),
            'event_type': data.get('event_type', ''),
            'location': data.get('location', ''),
            'user_action_detail': data.get('user_action_detail', ''),
            'session_number': data.get('session_number', ''),
            'localization_id': data.get('localization_id', ''),
            'ga_session_id': data.get('ga_session_id', ''),
            'value': data.get('value', 0.0),
            'state': data.get('state', 0.0),
            'engagement_time_msec': data.get('engagement_time_msec', 0.0),
            'current_progress': data.get('current_progress', ''),
            'event_origin': data.get('event_origin', ''),
            'place': data.get('place', 0.0),
            'selection': data.get('selection', ''),
            'analytics_storage': data.get('analytics_storage', ''),
            'browser': data.get('browser', ''),
            'install_store': data.get('install_store', ''),
            'user_params': data.get('user_params', ''),

        }

        yield transformed_data
def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project=PROJECT,
        job_name='your-job-name',
        temp_location='gs://{}/tmp'.format(BUCKET),
        region='us-central1',
        # setup_file='./setup.py'
    )

    with beam.Pipeline(options=options) as pipeline:
        # Read data from Cloud Storage
        data = (
            pipeline
            | 'Read from GCS' >> beam.io.ReadFromText(INPUT_FILE)
        )

        # Apply transformations
        transformed_data = (
            data
            | 'Transform Data' >> beam.ParDo(TransformData())
        )

        # Write the results to BigQuery
        transformed_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table=OUTPUT_TABLE,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )

if __name__ == '__main__':
    run()


