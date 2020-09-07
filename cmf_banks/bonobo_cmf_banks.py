import bonobo
import pandas as pd
import datetime as dt
import random as rd
import time
import os
from os import listdir
import glob
import requests
import zipfile
import pathlib
import sys

# connection to Data Base
sys.path.append("../conf")
from db_conn import engine

# get relative data folder
PATH = pathlib.Path(__file__).parent.resolve()

YEAR=2019
DIR_PATH = os.getcwd()


def get_stock_list():

    df=pd.read_excel(str(PATH.joinpath("./securities_data.xlsx")),converters={'SBIF_ID':str})
    ruts=(df[df.TIPO == "BANCO"]["RUTSD"]).to_list()
    sbif_ids=(df[df.TIPO == "BANCO"]["SBIF_ID"]).to_list()
    
    banks=zip(ruts,sbif_ids)
    
    yield from banks


def extract(*args):
    """
    get list of excel files 
    """
    bank_rut= args[0]
    bank_id= args[1]

    while True:
        try:
            print("Downloading file for..." + str(args[0]),end="\n")
            myfile = requests.get("https://www.sbif.cl/sbifweb/internet/bancos/balances/"+str(YEAR)+"/"+bank_id+".zip", allow_redirects=True)
            time.sleep(rd.randint(4,7))
            break
        except:
            print("request failed")
            pass
    
    open(str(PATH.joinpath("./data_banks/"+bank_id+".zip")), 'wb').write(myfile.content)
    time.sleep(rd.randint(1,2))
    
    yield (bank_rut,bank_id)
    

def process(*args):
    """
    Processing excel data
    """
    bank_rut= args[0]
    bank_id= args[1]

    print("Unzipping file for..."+str(bank_rut),end="\n")
    with zipfile.ZipFile(str(PATH.joinpath('./data_banks/'+bank_id+'.zip')), 'r') as zip_ref:
        zip_ref.extractall(str(PATH.joinpath('./data_banks/')))
    time.sleep(rd.randint(2,5))

    print("Processing File for..." + str(bank_rut),end="\n")

    df=pd.read_excel(str(PATH.joinpath('./data_banks/'+bank_id+'.xlsx')),skiprows=3,sheet_name="Resultados")
    df=df[df.columns[4:]]

    df.set_index("Descripción",inplace=True)
    df.index.name="fecha"

    df=df.T[["TOTAL INGRESOS OPERACIONALES","UTILIDAD (PERDIDA) CONSOLIDADA DEL EJERCICIO"]]*1000000
    df=df.reset_index()

    # rename columns
    df.columns=["fecha","ingreso","ganancia"]

    # add variables and transform
    df["moneda"]="PESOS"
    df["fecha_inicio"] = pd.to_datetime(df["fecha"].apply(lambda x: dt.date(x.year,1,1)))
    df["fecha_termino"] = df['fecha']
    df["rut"] = bank_rut

    yield (df, bank_rut)


def load(*args):
    """
    Save dataframe to DB

    Args:
        1: DataFrame - Data from yahoo finance
        2: String - Yahoo IDs
    """

    #args[0].to_csv(str(PATH.joinpath('./data/{}.csv'.format(args[1]))),index=False)

    try: # it will fail if duplicates
        args[0].to_sql('cmf', con=engine, if_exists='append', index=False)
    except:
        pass

    
def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(get_stock_list,extract, process, load)

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
    
    test = str(PATH.joinpath("./data_banks/*"))
    r = glob.glob(test)
    for i in r:
        os.remove(i)

    test = str(PATH.joinpath("./data/*"))
    r = glob.glob(test)
    for i in r:
        os.remove(i)
    
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )