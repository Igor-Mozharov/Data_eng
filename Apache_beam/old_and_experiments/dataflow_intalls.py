import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'formal-grammar-407607-97b29afb27d0.json'

PROJECT = 'formal-grammar-407607'
BUCKET = 't_data_bucket'
INPUT_FILE = 'gs://t_data_bucket/2023-12-13/installs.json'.format(BUCKET)
OUTPUT_TABLE = '{}.test_dataset.your_table'.format(PROJECT)

table_schema = {
    "fields": [
        {"name": "install_time", "type": "TIMESTAMP"},
        {"name": "marketing_id", "type": "STRING"},
        {"name": "channel", "type": "STRING"},
        {"name": "medium", "type": "STRING"},
        {"name": "campaign", "type": "STRING"},
        {"name": "keyword", "type": "STRING"},
        {"name": "ad_content", "type": "STRING"},
        {"name": "ad_group", "type": "STRING"},
        {"name": "landing_page", "type": "STRING"},
        {"name": "sex", "type": "STRING"},
        {"name": "alpha_2", "type": "STRING"},
        {"name": "alpha_3", "type": "STRING"},
        {"name": "flag", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "numeric", "type": "INTEGER"},
        {"name": "official_name", "type": "STRING"}
    ]
}

class TransformData(beam.DoFn):
    def process(self, element):
        data = json.loads(element)
        transformed_data = {
            'install_time': data.get('install_time'),
            'marketing_id': data.get('marketing_id'),
            'channel': data.get('channel'),
            'medium': data.get('medium'),
            'campaign': data.get('campaign'),
            'keyword': data.get('keyword'),
            'ad_content': data.get('ad_content'),
            'ad_group': data.get('ad_group'),
            'landing_page': data.get('landing_page'),
            'sex': data.get('sex'),
            'alpha_2': data.get('alpha_2'),
            'alpha_3': data.get('alpha_3'),
            'flag': data.get('flag'),
            'name': data.get('name'),
            'numeric': data.get('numeric'),
            'official_name': data.get('official_name')
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


