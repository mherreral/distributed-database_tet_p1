#!/usr/bin/env python3

import requests
import json
import base64

url = 'http://localhost:8000'


def dd(str):
    print(str)
    exit()


def encode_b64(data):
    return base64.b64encode(data).decode('utf-8')


def decode_b64(data):
    return base64.b64decode(data.encode('utf-8'))


def input_to_json(input_file):
    f = open(input_file, "rb")
    input_content = f.read()
    input_name = f.name
    f.close()

    encoded_data = encode_b64(input_content)
    json_data = {'key': input_name, 'value': encoded_data}
    return json_data


def json_to_output(json_data):

    output_name = json_data['key']
    encoded_content = json_data['value']
    output_content = decode_b64(encoded_content)

    w = open(f'./www/{output_name}.out', 'wb')
    w.write(output_content)
    w.close()


def put(input_file):
    data = input_to_json(input_file)
    data['operation'] = 'PUT'
    response = requests.post(f'{url}/put', data=data)
    print(response.status_code)


def get(key):
    data = {'key': key, 'operation': 'GET'}
    response = requests.post(f'{url}/get', data=data)
    print(response.status_code)


def update(input_file):
    data = input_to_json(input_file)
    data['operation'] = 'UPDATE'
    response = requests.post(f'{url}/update', data=data)
    print(response.status_code)


def delete(key):
    data = {'key': key, 'operation': 'DELETE'}
    response = requests.post(f'{url}/delete', data=data)
    print(response.status_code)


if __name__ == '__main__':
    from sys import argv
    # TODO params
    if len(argv) == 2:
        put(input_file=str(argv[1]))
    else:
        exit()
