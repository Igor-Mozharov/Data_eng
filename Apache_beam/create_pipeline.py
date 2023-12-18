import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import json
from table_schemas import installs_schema, costs_schema, events_schema, orders_schema
from datetime import datetime, timedelta


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'formal-grammar-407607-97b29afb27d0.json'
date_yesterday = (datetime.now() - timedelta(days=1)).date().strftime('%Y-%m-%d')

PROJECT = 'formal-grammar-407607'
BUCKET = 't_data_bucket'
INPUT_FILE_INSTALLS = f'gs://{BUCKET}/{date_yesterday}/installs.json'
OUTPUT_TABLE_INSTALLS = f'{PROJECT}.holy_dataset.installs-{date_yesterday}'
INPUT_FILE_COSTS = f'gs://{BUCKET}/{date_yesterday}/costs.json'
OUTPUT_TABLE_COSTS = f'{PROJECT}.holy_dataset.costs-{date_yesterday}'
INPUT_FILE_EVENTS = f'gs://{BUCKET}/{date_yesterday}/events.json'
OUTPUT_TABLE_EVENTS = f'{PROJECT}.holy_dataset.events-{date_yesterday}'
INPUT_FILE_ORDERS = f'gs://{BUCKET}/{date_yesterday}/orders.json'
OUTPUT_TABLE_ORDERS = f'{PROJECT}.holy_dataset.orders-{date_yesterday}'


class TransformData(beam.DoFn):
    def __init__(self, schema_table):
        self.schema_table = schema_table
    def process(self, element):
        data = json.loads(element)
        transformed_data = {}
        for item in self.schema_table['fields']:
            transformed_data[item['name']] = data.get(item['name'])
        yield transformed_data

class TransformData_OnlyOrders(beam.DoFn):
    def process(self, element):
        data = json.loads(element)
        transformed_data_list = []
        for key, value in data.items():
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
        job_name='best-job',
        temp_location='gs://{}/tmp'.format(BUCKET),
        # staging_location='gs://{}/stage'.format(BUCKET),
        region='us-central1',
        # save_main_session=True,
        # setup_file='./setup.py'
        # enable_streaming_engine=True
    )

    with beam.Pipeline(options=options) as pipeline:
        # Read data from Cloud Storage
        data_orders = (
            pipeline
            | 'Read from GCS_Orders' >> beam.io.ReadFromText(INPUT_FILE_ORDERS)
        )
        data_costs = (
            pipeline
            | 'Read from GCS_Costs' >> beam.io.ReadFromText(INPUT_FILE_COSTS)
        )
        data_installs = (
            pipeline
            | 'Read from GCS_Installs' >> beam.io.ReadFromText(INPUT_FILE_INSTALLS)
        )
        data_events = (
            pipeline
            | 'Read from GCS_Events' >> beam.io.ReadFromText(INPUT_FILE_EVENTS)
        )

        # Apply transformations
        transformed_data_orders = (
            data_orders
            | 'Transform Data_Orders' >> beam.ParDo(TransformData_OnlyOrders())
        )
        transformed_data_costs = (
            data_costs
            | 'Transform Data_Costs' >> beam.ParDo(TransformData(costs_schema))
        )
        transformed_data_installs = (
            data_installs
                | 'Transform Data_Installs' >> beam.ParDo(TransformData(installs_schema))
        )
        transformed_data_events = (
            data_events
            | 'Transform Data_Events' >> beam.ParDo(TransformData(events_schema))
            )

        # Write the results to BigQuery
        transformed_data_orders | 'Write to BigQuery_Orders' >> beam.io.WriteToBigQuery(
            table=OUTPUT_TABLE_ORDERS,
            schema=orders_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )
        transformed_data_costs | 'Write to BigQuery_Costs' >> beam.io.WriteToBigQuery(
            table=OUTPUT_TABLE_COSTS,
            schema=costs_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )
        transformed_data_installs | 'Write to BigQuery_Installs' >> beam.io.WriteToBigQuery(
            table=OUTPUT_TABLE_INSTALLS,
            schema=installs_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )
        transformed_data_events | 'Write to BigQuery_Events' >> beam.io.WriteToBigQuery(
            table=OUTPUT_TABLE_EVENTS,
            schema=events_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )

if __name__ == '__main__':
    run()


