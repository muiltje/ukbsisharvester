import logging
from harvest import harvest_yesterday
import config
from datetime import date

if __name__ == '__main__':
    logfile = date.today().strftime('%Y-%m-%d_') + config.LOGFILE_SUFFIX
    logging.basicConfig(filename=logfile, filemode='a')
    logger = logging.getLogger(__name__)
    logger.setLevel(config.LOG_LEVEL)

    harvest_yesterday()
