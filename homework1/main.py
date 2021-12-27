import os

from datetime import date
from file_manager import Filemanager
from file_manager_exception import FileManagerException
from http_request_helper import HttpRequestHelper
from config import Config
from http_request_helper_exception import HttpRequestHelperException


def app(file_manager, request_helper, process_date=None):
    if not process_date:
        process_date = str(date.today())

    try:
        token = request_helper.__prepare_access_token__()
        response = request_helper.__get_data__(token)

        file_manager.__create_directory__(process_date)
        file_manager.__save_data__(response, process_date)
    except (HttpRequestHelperException, FileManagerException) as err:
        print("Error happened during processing data")


if __name__ == '__main__':
    config = Config(os.path.join('..', 'config.yaml'))
    file_manager = Filemanager(config.get_config('currency_app'))
    request_helper = HttpRequestHelper(config.get_config('currency_app'))

    date = ['2021-12-01', '2021-12-02', '2021-12-03']
    for dt in date:
        app(file_manager=file_manager, request_helper=request_helper, process_date=dt)
