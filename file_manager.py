import os
import json

from file_manager_exception import FileManagerException


class Filemanager:
    def __init__(self, config):
        self.config = config

    def __create_directory__(self, dir_name):
        directory = os.path.join(self.config['directory'], dir_name)
        try:
            os.makedirs(directory, exist_ok=True)
        except (FileNotFoundError, PermissionError) as err:
            print("Could not create directory " + directory)
            raise FileManagerException(err)

    def __save_data__(self, data_json, process_date):
        try:
            directory = os.path.join(self.config['directory'], process_date)
            with open(os.path.join(directory, process_date + '.json'), 'w') as json_file:
                json.dump(data_json, json_file)
        except Exception as exc:
            print("Could not write data into file " + os.path.join(directory, process_date + '.json'))
            raise FileManagerException(exc)
