import os

from datetime import date
from file_manager import Filemanager
from http_request_helper import HttpRequestHelper
from config import Config


def app(file_manager, request_helper, process_date=None):
    if not process_date:
        process_date = str(date.today())

    token = request_helper.__prepare_access_token__()
    response = request_helper.__get_data__(token)

    file_manager.__create_directory__(process_date)
    file_manager.__save_data__(response, process_date)


if __name__ == '__main__':
    config = Config(os.path.join('.', 'config.yaml'))
    file_manager = Filemanager(config.get_config('currency_app'))
    request_helper = HttpRequestHelper(config.get_config('currency_app'))

    date = ['2021-06-24', '2021-06-19', '2021-06-20', '2021-06-21']
    for dt in date:
        app(file_manager=file_manager, request_helper=request_helper, process_date=dt)
