import os
import json


class Filemanager:
    def __init__(self, config):
        self.config = config

    def __create_directory__(self, dir_name):
        os.makedirs(os.path.join(self.config['directory'], dir_name), exist_ok=True)

    def __save_data__(self, data_json, process_date):
        directory = os.path.join(self.config['directory'], process_date)
        with open(os.path.join(directory, process_date + '.json'), 'w') as json_file:
            json.dump(data_json, json_file)

