import bonobo
import pandas as pd
from datetime import datetime
import random as rd
import sys
import time
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import os
import glob
import pathlib
import sys

# connection to Data Base
sys.path.append("../conf")
from db_conn import engine

# get relative data folder
PATH = pathlib.Path(__file__).parent.resolve()

LIST_YEAR = [2018, 2019]

def get_year():
    yield from LIST_YEAR


def getExcelCMF(year):
    '''
    Use Selenium to get and save excel files from CMF Website. 
    25 excel files per year
    '''
    

    # ID of 
    ruts=pd.read_excel(str(PATH.joinpath("./securities_data.xlsx")))#[0:1]
    ruts=ruts[ruts.TIPO != "BANCO"]["RUTSD"].to_list()

    """
    Downloads Excel Files from CMF for each company in ruts list
    """
    
   
    #Setting up Chrome
    options = webdriver.ChromeOptions()
    
    print("DOWNLOAD DIRECTORY", str(PATH.joinpath("./data_non_banks")))
    prefs = {'download.default_directory' : str(PATH.joinpath("./data_non_banks"))}
    options.add_experimental_option('prefs', prefs)
    
    #driver = webdriver.Chrome(options=options,executable_path=str(PATH.joinpath("./tools/chromedriver.exe")))
    driver = webdriver.Chrome(options=options,executable_path=str(PATH.joinpath("./tools/chromedriver")))

    driver.maximize_window()
    time.sleep(5)

    for rut in ruts:

        print("Getting data for " + str(rut) + "...for year..." + str(year) + "from CMF Website")

        while True:
            #Setting and accessing AAFM Website 
            try:
                driver.get('http://www.cmfchile.cl/institucional/estadisticas/merc_valores/sa_eeff_ifrs/sa_eeff_ifrs_index.php?lang=es&rg_rf=RVEMI')
                time.sleep(10) 

            
                select = Select(driver.find_element_by_xpath('/html/body/div[1]/div[4]/div[4]/div[4]/div/form[1]/table/tbody/tr[1]/td[2]/select'))
                select.deselect_all()
                select.select_by_value(str(rut))

                time.sleep(5) 

                select = Select(driver.find_element_by_xpath('/html/body/div[1]/div[4]/div[4]/div[4]/div/form[1]/table/tbody/tr[3]/td[2]/div[2]/table/tbody/tr/td[1]/select[1]'))
                select.select_by_value('09')

                select = Select(driver.find_element_by_xpath('/html/body/div[1]/div[4]/div[4]/div[4]/div/form[1]/table/tbody/tr[3]/td[2]/div[2]/table/tbody/tr/td[1]/select[2]'))
                select.select_by_value(str(year))
                time.sleep(5) 

                select = Select(driver.find_element_by_xpath('/html/body/div[1]/div[4]/div[4]/div[4]/div/form[1]/table/tbody/tr[3]/td[2]/div[2]/table/tbody/tr/td[2]/span/select[1]'))
                select.select_by_value('12')

                select = Select(driver.find_element_by_xpath('/html/body/div[1]/div[4]/div[4]/div[4]/div/form[1]/table/tbody/tr[3]/td[2]/div[2]/table/tbody/tr/td[2]/span/select[2]'))
                select.select_by_value(str(year))

                activeElement=driver.find_element_by_xpath("/html/body/div[1]/div[4]/div[4]/div[4]/div/form[1]/table/tbody/tr[4]/td[2]/span[1]/input[2]")
                activeElement.click()

                activeElement=driver.find_element_by_xpath("/html/body/div[1]/div[4]/div[4]/div[4]/div/form[1]/table/tbody/tr[5]/td[2]/input")
                activeElement.click()

                time.sleep(10) 

                activeElement=driver.find_element_by_xpath("/html/body/div[1]/div[4]/div[3]/div[5]/table/tbody/tr[1]/td[2]/a[1]")
                activeElement.click()
                
                time.sleep(rd.randint(10,15)) 
                
                list_of_files = glob.glob(str(PATH.joinpath("./data_non_banks"))+"/*.xlsx") # * means all if need specific format then *.csv
                
                latest_file = max(list_of_files, key=os.path.getctime)
                

                print("Latest File Downloaeded for year " + str(year)+"...." + latest_file)
                print("From " + latest_file)
                print("TO   " +  str(PATH.joinpath("./data_non_banks/")) +"/"+str(rut)+"-"+str(year)+".xlsx")
                print()
                os.rename( latest_file, str(PATH.joinpath("./data_non_banks")) +"/"+str(rut)+"-"+str(year)+".xlsx")
                break

            except:
                print("Unexpected error:", sys.exc_info()[0])
                pass


    driver.quit()
    print("Process successful")
    yield year  


def find_excel_filenames(year):
    filenames = os.listdir(PATH.joinpath("./data_non_banks"))
    return [ filename for filename in filenames if filename.endswith( str(year)+".xlsx" ) ]


def extract(year):
    """
    get list of excel files 
    """
    file_list = find_excel_filenames(year)
    print(file_list)
    yield from file_list


def process(*args):
    """
    Proceses the files downloaded from CMF and extracts relevant information
    """
    
    year=(args[0].split("-")[1]).split(".")[0]
    print("Printing year ...."+ year)

    df= pd.read_excel(str(PATH.joinpath("./data_non_banks")) + "/"+args[0],skiprows=11)
    df=df[['Fecha',
    'Moneda',
    'FechaInicio',
    'FechaCierre',
    'Ingresosdeactividadesordinarias',
    'Ganancia(pérdida)'    
    ]]

    df.columns=['fecha',
    'moneda',
    'fecha_inicio',
    'fecha_termino',
    'ingreso',
    'ganancia'    
    ]

    df['fecha'] = df['fecha_termino']
    stock_id = int(args[0].split("-")[0])
    df["rut"]=stock_id
    
    print("stock rut", stock_id)

    yield (df, stock_id,year)


def load(*args):
    """
    Save dataframe as csv file

    Args:
        0: DataFrame - Data from CMF
        1: RUT
        2: Year
    """

    #args[0].to_csv(PATH.joinpath('./data/{}-{}.csv'.format(args[1],args[2])),index=False)

    try:
        args[0].to_sql('cmf', con=engine, if_exists='append', index=False)
    except:
        pass


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(get_year, getExcelCMF,extract, process, load)

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
    

    test = str(PATH.joinpath("./data_non_banks/*"))
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