from datetime import date
import config
import logging
from harvest import initial_harvest

"""
This makes initial harvest until today
"""

if __name__ == '__main__':
    logfile = date.today().strftime('%Y-%m-%d_') + config.LOGFILE_SUFFIX
    logging.basicConfig(filename=logfile, filemode='a')
    logger = logging.getLogger(__name__)
    logger.setLevel(config.LOG_LEVEL)

    initial_harvest('2020-05-01')
