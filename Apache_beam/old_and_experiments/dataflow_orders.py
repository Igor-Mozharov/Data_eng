import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import json
from datetime import datetime

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'formal-grammar-407607-97b29afb27d0.json'

PROJECT = 'formal-grammar-407607'
BUCKET = 't_data_bucket'
INPUT_FILE = 'gs://t_data_bucket/2023-12-13/orders.json'.format(BUCKET)
OUTPUT_TABLE = '{}.holy_dataset.your_table'.format(PROJECT)

table_schema = {
    'fields': [
        {'name': 'event_time', 'type': 'INTEGER'},
        {'name': 'transaction_id', 'type': 'STRING'},
        {'name': 'type', 'type': 'STRING'},
        {'name': 'origin_transaction_id', 'type': 'STRING'},
        {'name': 'category', 'type': 'STRING'},
        {'name': 'payment_method', 'type': 'STRING'},
        {'name': 'fee', 'type': 'FLOAT'},
        {'name': 'tax', 'type': 'FLOAT'},
        {'name': 'iap_item_name', 'type': 'STRING'},
        {'name': 'iap_item_price', 'type': 'FLOAT'},
        {'name': 'discount_code', 'type': 'STRING'},
        {'name': 'discount_amount', 'type': 'FLOAT'},
    ]
}

class TransformData(beam.DoFn):
    def process(self, element):
        data = json.loads(element)
        transformed_data_list = []

        for key, value in data.items():
            # Iterate over the keys (e.g., '0', '1', etc.) for each field
            for sub_key, sub_value in value.items():
                transformed_data = {
                    'event_time': data['event_time'].get(sub_key),
                    'transaction_id': data['transaction_id'].get(sub_key),
                    'type': data['type'].get(sub_key),
                    'origin_transaction_id': data['origin_transaction_id'].get(sub_key),
                    'category': data['category'].get(sub_key),
                    'payment_method': data['payment_method'].get(sub_key),
                    'fee': data['fee'].get(sub_key),
                    'tax': data['tax'].get(sub_key),
                    'iap_item_name': data['iap_item_name'].get(sub_key),
                    'iap_item_price': data['iap_item_price'].get(sub_key),
                    'discount_code': data['discount_code'].get(sub_key),
                    'discount_amount': data['discount_amount'].get(sub_key),
                }
                if transformed_data not in transformed_data_list:
                    transformed_data_list.append(transformed_data)

        return transformed_data_list
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

####not done
