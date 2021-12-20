import requests
import json
import os


class HttpRequestHelper:
    def __init__(self, config):
        self.config = config

    def __prepare_access_token__(self):
        url = self.config['url']['auth']
        headers = {"content-type": "application/json"}
        data = {"username": "rd_dreams", "password": "djT6LasE"}
        r = requests.post(url, headers=headers, data=json.dumps(data))
        return r.json()['access_token']

    def __get_data__(self, token):
        url = self.config['url']['data']
        headers = {"content-type": "application/json",
                   "Authorization": "JWT " + token}
        data = {"date": "2021-10-08"}
        return requests.get(url, headers=headers, data=json.dumps(data)).json();

