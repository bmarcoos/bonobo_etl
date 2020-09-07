import bonobo
import pandas as pd
from datetime import datetime
from pandas_datareader import data as pdr
import yfinance as yf
from random import randint
import time
import yaml
import sys

sys.path.append("../conf")
from db_conn import engine

yf.pdr_override()

# set dates to retrieve stock data
DT_START = datetime(2018,1,1)
DT_END = datetime.now()

def get_stock_list():
    """
    Get list of stocks and yahoo ids
    """
    stock_ids = pd.read_csv('stock ids.csv', sep=';')
    list_stocks = stock_ids['YAHOO'].to_list()

    yield from list_stocks


def extract(stock_id):
    """
    Get data from yahoo finance for given stock
    """

    # wait a random time to avoid get banned from api service
    time.sleep(randint(2,8))
    
    df = pdr.get_data_yahoo(stock_id, start=DT_START, end=DT_END)
    
    yield stock_id, df


def process(stock_id, df):
   '''
   Filter DF columns and add a column with stock:id
   '''

   df = df[['Close', 'Volume' ]]
   df['stock_id'] = stock_id

   return stock_id, df


def load(stock_id, df):
    """
    Save dataframe as csv file

    Args:
        1: DataFrame - Data from yahoo finance
            Example:
                        Close    Volume    stock_id
            Date                                       
            2020-05-04  19.240000    978481  VAPORES.SN
            2020-05-05  19.420000    298938  VAPORES.SN

        2: String - Yahoo IDs
    """

    df.to_sql('yahoo_prices', con=engine, if_exists='append', index=True)

    #df.to_csv('data/{}.csv'.format(stock_id))


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(get_stock_list, extract, process, load)

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