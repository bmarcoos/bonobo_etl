import bonobo
import csv
from datetime import datetime
import requests
import re
import sys
import sqlalchemy as db

sys.path.append("../conf")
from db_conn import engine

START_TIME = 2018
END_TIME = datetime.now().date().year

DICT_MONTHS = {
    'Jan':1,
    'Feb':2,
    'Mar':3,
    'Apr':4,
    'May':5,
    'Jun':6,
    'Jul':7,
    'Aug':8,
    'Sep':9,
    'Oct':10,
    'Nov':11,
    'Dec':12
}

# connection to Data Base
connection = engine.connect()
metadata = db.MetaData(bind=engine)
ocde = db.Table('ocde', metadata, autoload=True)


def get_api_links():
    '''
    Get api links
    '''

    with open('ocde_api_links.csv', 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        next(reader)

        for variable_name, link in reader:
            yield variable_name, link.format(START_TIME, END_TIME)
       

def extract(variable_name, link):
    '''
    Extract data form OCDE API.

    Parameters:
        variable_name: String -  name of the varaible
        link: String - API link to get data from

    Return:
        period: String - reference period
            Example:
                2018
                Q4-2019
                Jan-2020
        value: Float - value associated to the variable
    '''
   
    # request data
    res = requests.get(link).json()

    # get values
    data_sets = res['dataSets'][0]['series']
    values = []
    for v in data_sets.values():
        for ob in v["observations"].values():
            values.append(ob[0])
            
    # get variable names
    dimensions = res["structure"]["dimensions"]["observation"][0]["values"]
    list_names = []
    for d in dimensions:
        list_names.append(d['name'])

    # zip names and values and iterate
    for period, value in zip(list_names, values):
        yield variable_name, period, value


def transform(variable_name, period, value):
    """
    Apply transformations and add period type

    Example:
        2019      -> Anual
        Q1-2017   -> Trimestral
        Jan-2017  -> Mensual
    """

    period_type, year, quarter, month = None, None, None, None
    if re.search("Q[1-4]{1}-\d{4}", period):
        period_type = "Trimestral"
        quarter = period[1]
        year = int(period[-4:])
    elif re.search("[A-Z][a-z][a-z]-\d\d", period):
        period_type = "Mensual"
        year = int(period[-4:])
        month = DICT_MONTHS[period[0:3]]
    elif re.search("20\d\d", period):
        period_type = "Anual"
        year = int(period)

    value = round(value, 6)

    yield variable_name, period, period_type, year, quarter, month, value


def load(variable_name, period, period_type, year, quarter, month, value):
    """
    Load data to Database. Table ocde.
    """
    
    print(variable_name, period, period_type, year, quarter, month, value)

    connection.execute(ocde.insert(), {"variable": variable_name,
                                         "period": period,
                                         "period_type": period_type,
                                         "year": year,
                                         "quarter": quarter,
                                         "month": month,
                                         "value": value
                                        })

def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(get_api_links, extract, transform, load)

    return graph


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """
    return {}


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )