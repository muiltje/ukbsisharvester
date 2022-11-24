""" CSV File handling made simple """
import csv
import logging
from datetime import date
import config
import sys


logger = logging.getLogger(__name__)
consoleHandler = logging.StreamHandler(sys.stdout)
log_format = "%(asctime)s - %(levelname)-8s - %(name)s | %(message)s"
logFormatter = logging.Formatter(log_format)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

class CsvWriter:

    def __init__(self, start_date, end_date, fields, max_records_file, dir):

        self._start_date = start_date
        self._end_date = end_date
        self._fields_name = fields
        self._max_records_file = max_records_file
        self._dir = dir
        self._write_count = 0
        self._file_count = 1
        self._file = False
        self._writer = False

    def write_record_to_csv(self, record_data):
        """
        Write a record to CSV,
        """
        # If the file has reached the max allowed records create a new
        if self._file is not False and (self._write_count % self._max_records_file) == 0:
            # Get another file
            self._file.close()
            self._file_count += 1

        # if there is no file yet or is closed create a new file
        if self._file is False or self._file.closed:
            self.create_csv_writer()

        self._writer.writerow(record_data)
        self._write_count += 1

    # returns a CSV file with writer instance
    def create_csv_writer(self):
        # file name
        file_name = '%s/%s-%s_%d.csv' % (
            self._dir, self._start_date.strftime('%Y%m%d'), self._end_date.strftime('%Y%m%d'), self._file_count
        )
        if self._write_count > 0 :
            logger.info('Witten %s records', self._write_count)
        logger.info('Creating file %s', file_name)

        # create a new file
        self._file = open(file_name, 'w', newline='', encoding='utf8')
        fieldnames = list(self._fields_name.keys())
        self._writer = csv.DictWriter(self._file, fieldnames=fieldnames, delimiter='$', quotechar='"')
        self._writer.writeheader()

    def close_file(self):
        if self._file is not False:
            self._file.close()
