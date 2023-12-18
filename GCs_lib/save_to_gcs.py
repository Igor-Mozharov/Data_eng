import requests
import json
from google.cloud import storage
from io import BytesIO
from time import sleep
import pyarrow.parquet as pq
from datetime import datetime, timedelta

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/"

header = {
    "Authorization": "gAAAAABldBifhsdPuk1ssI8plxUobioEgzFtKl1JIHp21fdJlVUK2AMPdXvy0FQOPILRLUtSosGSi9cO"
                     "-yAZltWDuURZJFJ2UtP83i82sy0_KUhPu8v7VZbhExT1Prh3o6TgZDzVi00ImtRy0yyHOGyXDyCS6NvEqw=="}

date_yesterday = (datetime.now() - timedelta(days=1)).date().strftime('%Y-%m-%d')

params = {"date": date_yesterday}

api_method = {
    'download_installs': 'installs',
    'download_costs': 'costs',
    'download_events': 'events',
    'download_orders': 'orders'
}

save_folder = './downloaded_data/'
gcp_folder = f'{date_yesterday}/'

bucket_name = 't_data_bucket'

#save local
def save_to_json(filename, data, app_method):
    with open(file=filename, mode='w', encoding='utf-8') as file:
        if isinstance(data, list):
            for item in data:
                json.dump(item, file)
                file.write('\n')
        else:
            json.dump(data, file)
    print(f'{app_method} ---> Done!')

#save to gcp
def save_to_gcp(data, app_method):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcp_folder + app_method + '.json')
    with blob.open(mode='w') as file:
        if isinstance(data, list):
            for item in data:
                json.dump(item, file)
                file.write('\n')
        else:
            json.dump(data, file)
    print(f'{app_method} ---> Done!')


def download_installs():
    response_count = 0
    while response_count < 1:
        response = requests.get(url + api_method['download_installs'], headers=header, params=params)
        if response.status_code == 200:
            data = response.json()
            data = data['records']
            data = json.loads(data)
            save_to_json(save_folder + api_method['download_installs'] + '.json', data, api_method['download_installs'])
            # save_to_gcp(data, api_method['download_installs'])
            response_count += 1
        else:
            print(f'Error: Status code - {response.status_code}')
            sleep(1)


def download_costs():
    dimensions_dict = {}
    dimensions_list = ['location', 'channel', 'medium', 'campaign', 'keyword', 'ad_content', 'ad_group', 'landing_page']
    for dimension in dimensions_list:
        request_count = 0
        while request_count < 1:
            params['dimensions'] = dimension
            response = requests.get(url + api_method['download_costs'], headers=header, params=params)
            if response.status_code == 200:
                data = response.text
                lines = data.strip().split('\n')
                headers = lines[0].split('\t')
                headers = ['name', headers[1]]
                res = [dict(zip(headers, line.split('\t'))) for line in lines[1:]]
                dimensions_dict[dimension] = res
                request_count += 1
                print(f'Downloading cost for {dimension}')
            else:
                print(f'Error: Status code - {response.status_code}')
                sleep(1)
    save_to_json(save_folder + api_method['download_costs'] + '.json', dimensions_dict, api_method['download_costs'])
    # save_to_gcp(dimensions_dict, api_method['download_costs'])


def download_events():
    all_events = []
    while True:
        response = requests.get(url + api_method['download_events'], headers=header, params=params)
        if response.status_code == 200:
            data = response.json()
            all_events.extend(json.loads(data.get('data', [])))
            next_page = data.get('next_page')
            print(f'Downloading next page - {next_page}')
            if not next_page:
                break
            else:
                params['next_page'] = next_page
        else:
            print(f'Error: Status code - {response.status_code}')
            sleep(1)
    save_to_json(save_folder + api_method['download_events'] + '.json', all_events, api_method['download_events'])
    # save_to_gcp(all_events, api_method['download_events'])


def download_orders():
    response_count = 0
    while response_count < 1:
        response = requests.get(url + api_method['download_orders'], headers=header, params=params)
        if response.status_code == 200:
            parquet_data = BytesIO(response.content)
            table = pq.read_table(parquet_data)
            to_pandas = table.to_pandas()
            to_json = to_pandas.to_json()
            res = json.loads(to_json)
            res = {key.replace('.', '_'): value for key, value in res.items()}
            save_to_json(save_folder + api_method['download_orders'] + '.json', res, api_method['download_orders'])
            # save_to_gcp(to_json, api_method['download_orders'])
            response_count += 1
        else:
            print(f' Status code - {response.status_code}')
            sleep(1)


if __name__ == '__main__':
    download_installs()
    # sleep(1)
    # download_orders()
    # sleep(1)
    # download_costs()
    # sleep(1)
    # download_events()
