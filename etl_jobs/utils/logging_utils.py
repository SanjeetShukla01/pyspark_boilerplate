"""
# logging_utils.py for project logger_class
# Created by @Sanjeet Shukla at 6:35 PM 4/22/2022 using PyCharm
# This file contains logger class.
"""
import pathlib
from datetime import datetime
import logging
import os
import sys

class Logger():

    # To create a singleton class
    _unique_instance = None

    # Maximum number of logs to store
    LOGS_COUNT = 10

    def __init__(self, logger_name):
        """
        Constructor.
        Takes logger_name as an argument and optional argument days_to_keep (default 5).
        Logs older than the current date minus days_to_keep will be erased
        for maintenance purposes.
        Calls to __verify_path() and to __clean_old_logs().
        """
        self.FORMATTER = logging.Formatter('%(levelname)s - %(asctime)s {%(pathname)s:%(lineno)d}: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
        self.LOGS_DIRECTORY = os.path.join(os.path.dirname(os.path.dirname(__file__)), '../logs/')
        if not os.path.exists(self.LOGS_DIRECTORY):
            os.mkdir(self.LOGS_DIRECTORY)
        self.FILE_NAME = datetime.now().strftime('log_%H_%M_%S_%d_%m_%Y.log')
        self.LOG_FILE = self.LOGS_DIRECTORY + self.FILE_NAME
        self.logger_name = logger_name
        self.__clean_old_logs()

    @classmethod
    def get_instance(cls):
        if not cls._unique_instance:
            cls._unique_instance = cls()
        return cls._unique_instance

    def get_console_handler(self):
        """
        This function returns the handler for the logger
        :return: returns stream handler for logger
        """
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.FORMATTER)
        return console_handler

    def get_file_handler(self):
        """
        This function returns the file handler object which rotates the log file at midnight of each day
        :return: file handler with rotating log file configuration
        """
        file_handler = logging.FileHandler(self.LOG_FILE)
        file_handler.setFormatter(self.FORMATTER)
        return file_handler

    def get_logger(self):
        """
        This function returns the logger object with the handler and log rotation config_utils defined.
        :param logger_name: Name or file for which the logger is created.
        :return: logger object with the handler and log rotation config_utils defined.
        """
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(self.get_console_handler())
        logger.addHandler(self.get_file_handler())
        logger.propagate = False
        return logger

    def __clean_old_logs(self):
        for name in self.__get_old_logs():
            path = os.path.join(self.LOGS_DIRECTORY, name)
            self.__delete_file(path)

    def __get_old_logs(self):
        logs = [name for name in self.__get_file_names(self.LOGS_DIRECTORY)]
        logs.sort(reverse=True)
        return logs[self.LOGS_COUNT:]

    def __get_file_names(self, path):
        return [item.name for item in pathlib.Path(path).glob("*") if item.is_file()]

    def __delete_file(self, path):
        try:
            os.remove(path)
        except Exception as ex:
            print("Exception deleting log file " + str(ex))