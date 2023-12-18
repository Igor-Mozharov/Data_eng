import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'formal-grammar-407607-97b29afb27d0.json'

PROJECT = 'formal-grammar-407607'
BUCKET = 't_data_bucket'
INPUT_FILE = 'gs://t_data_bucket/2023-12-13/costs.json'.format(BUCKET)
OUTPUT_TABLE = '{}.test_dataset.your_table'.format(PROJECT)

table_schema = {
    "fields": [
        {"name": "ad_group", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "keyword", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "medium", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "landing_page", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "channel", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "ad_content", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "campaign", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "location", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]}
    ]
}

class TransformData(beam.DoFn):
    def process(self, element):
        data = json.loads(element)

        transformed_data = {
            'location': data.get('location', []),
            'channel': data.get('channel', []),
            'medium': data.get('medium', []),
            'campaign': data.get('campaign', []),
            'keyword': data.get('keyword', []),
            'ad_content': data.get('ad_content', []),
            'ad_group': data.get('ad_group', []),
            'landing_page': data.get('landing_page', []),
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


