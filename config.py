import yaml


class Config:
    def __init__(self, path):
        with open(path, 'r') as yaml_file:
            self.__config = yaml.load(yaml_file, Loader=yaml.FullLoader)

    def get_config(self, app_name):
        return self.__config.get(app_name)