#!/usr/bin/env python3
import requests.exceptions
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
import requests
import json
import base64
from argparse import ArgumentParser

url = ''
def check_server():

    logging.basicConfig(filename="Client.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

    logging.info(f'Checking server')


    global url
    #url = 'http://ec2-54-174-27-144.compute-1.amazonaws.com:80'
    #url = 'http://ec2-18-233-171-48.compute-1.amazonaws.com'
    url = 'http://localhost:8000'

    try:
        r = requests.get(url)
        r.raise_for_status()
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        print("Down")
        #url = 'http://ec2-52-90-136-52.compute-1.amazonaws.com'
        url = 'http://localhost:8001'
    except requests.exceptions.HTTPError:
        print("4xx, 5xx")
        #url = 'http://ec2-52-90-136-52.compute-1.amazonaws.com'
        url = 'http://localhost:8001'

    return url


def dd(str):
    logging.basicConfig(filename="Client.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

    logging.info(f'Dump and die')

    print(str)
    exit()


def encode_b64(data):
    logging.basicConfig(filename="Client.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

    logging.info(f'Encoding b64')

    return base64.b64encode(data).decode('utf-8')


def decode_b64(data):
    logging.basicConfig(filename="Client.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

    logging.info(f'Decoding b64')

    return base64.b64decode(data.encode('utf-8'))


def input_to_json(input_file):
    logging.basicConfig(filename="Client.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

    logging.info(f'Creting input JSON')

    f = open(input_file, "rb")
    input_content = f.read()
    input_name = f.name
    f.close()

    encoded_data = encode_b64(input_content)
    json_data = {'key': input_name, 'value': encoded_data}
    return json_data


def json_to_output(json_data):
    logging.basicConfig(filename="Client.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

    logging.info(f'Creating output JSON')

    output_name = json_data['key']
    encoded_content = json_data['value']
    output_content = decode_b64(encoded_content)

    w = open(f'{output_name}.out', 'wb')
    w.write(output_content)
    w.close()


def put(input_file):
    logging.basicConfig(filename="Client.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

    logging.info(f'Begenning put operation')

    data = input_to_json(input_file)
    data['method'] = 'put'
    response = requests.post(f'{url}', json=data)
    st_code = response.status_code
    if st_code == 200:
        print('Saved successfully')
    else:
        print('Something went wrong')


def get(key):
    logging.basicConfig(filename="Client.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

    logging.info(f'Begenning get operation')

    data = {'key': key, 'method': 'get'}
    response = requests.post(f'{url}', json=data)
    st_code = response.status_code
    returned_data = json.loads(response.text)
    if st_code == 200:
        json_to_output(returned_data)
        print('Successful request')
    else:
        print('Something went wrong')


def update(input_file):
    logging.basicConfig(filename="Client.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

    logging.info(f'Begenning update operation')

    data = input_to_json(input_file)
    data['method'] = 'update'
    response = requests.post(f'{url}', json=data)
    st_code = response.status_code
    if st_code == 200:
        print('Updated successfully')
    else:
        print('Something went wrong')


def delete(key):
    logging.basicConfig(filename="Client.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

    logging.info(f'Begenning delete operation')

    data = {'key': key, 'method': 'delete'}
    response = requests.post(f'{url}', json=data)
    st_code = response.status_code
    if st_code == 200:
        print('Deleted successfully')
    else:
        print('Something went wrong')


def arguments():
    parser = ArgumentParser()
    group = parser.add_mutually_exclusive_group()

    group.add_argument('-u', '--update',
                       help='Use for update value in DB',
                       action='store_true')
    group.add_argument('-d', '--delete',
                       help='Use for delete an entry in DB',
                       action='store_true')
    group.add_argument('-p', '--put',
                       help='Use to create an entry in DB',
                       action='store_true')
    group.add_argument('-g', '--get',
                       help='Use to return a value from DB',
                       action='store_true')

    parser.add_argument('path', type=str,
                        help='The name of the key or filename depending of the method')

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arguments()
    key = args.path
    check_server()
    if args.update:
        update(key)
    elif args.delete:
        delete(key)
    elif args.put:
        put(key)
    elif args.get:
        get(key)
    else:
        print('Something went wrong')

    logging.basicConfig(filename="Client.log",
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s", 
        datefmt='%m/%d/%Y %I:%M:%S %p'
        )

    logging.info(f"STARTED CLIENT")
