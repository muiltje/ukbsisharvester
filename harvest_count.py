import config
from harvest import initial_harvest
from datetime import date

if __name__ == '__main__':
    FILE_NAME = config.OUTPUT_DIR + date.today().strftime('/%Y-%m-%d_count.csv')

    initial_harvest('2020-05-01', True)
