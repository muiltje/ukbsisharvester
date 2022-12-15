from harvest import initial_harvest

"""
Harvest of all historical data until today

consists in a monthly harvest until current month and then a normal harvest until yesterday

date_from : should be in ISO format: YYYY-MM-DD, any date can be used

"""
if __name__ == '__main__':
    initial_harvest(date_from='2020-05-01')
