import logging
import azure.functions as func
import os
import sys
import json
import time

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)
from custom_module.adls_module import ADLSModule
from custom_module.selenium_scrapping import threaded_selenium_scrapping

def generate_start_urls(num_page, base_url_format):
    base_url_format+='/p{}'
    return [base_url_format.format(page) for page in range(1, num_page + 1)]

def create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Set default parameters if one of them is missing
    # Defualt: nthreads is 2, num_pages is 6, limit_each_page is 20
    nthreads = req.params.get('nthreads')
    num_pages = req.params.get('num_pages')
    limit_each_page = req.params.get('limit_each_page')
    base_url_format = req.params.get('base_url_format')

    nthreads = int(nthreads) if nthreads is not None else 2
    num_pages = int(num_pages) if num_pages is not None else 6
    limit_each_page = int(limit_each_page) if limit_each_page is not None else 20
    if(base_url_format is None):
        base_url_format='https://batdongsan.com.vn/ban-nha-rieng-tp-hcm'


    # Reading Azure Data Lake Storage Gen 2 (ADLS2) credentials from json file otherwise save to local
    try:
        create_folder('./scraped_data')
        create_folder('./reconciled_data')

        with open("./config.json", "r") as config_file:
            conf = json.load(config_file)
            connection_string = conf["adls"]["connection_string"]
            key = conf["adls"]["key"]
            sa_name = conf["adls"]["sa_name"]
            adls=ADLSModule(sa_name,connection_string,key)
            logging.info('Connection to ADLS created successfully. Scraped data will be saved to Bronze container in ADLS Gen2')
    except Exception as e:
            logging.info(f"ADLS failed to create: Error{e} \nScraped data will be saved to local folder")
            adls=None
    # adls=None

    # Starts main scrapping function
    try:
        logging.info('Starts scrapping process, please wait.....')
        
        # Call main function and calculate total time it takes
        start_time = time.time()
        start_urls = generate_start_urls(num_pages, base_url_format)
        threaded_selenium_scrapping(nthreads,start_urls,limit_each_page,adls)
        end_time = time.time()
        elapsed_time = round(end_time - start_time)

        response_mess=f"This HTTP triggered scrapping function executed successfully, it took total {elapsed_time}s to scrape {num_pages} pages with {nthreads} threads\nEach page contains {limit_each_page} properties"
        logging.info(response_mess)
        return func.HttpResponse(response_mess,status_code=200)
    except Exception as e:
        response_mess=f"Error happend in the scrapping process: {e}"
        logging.info(response_mess)
        return func.HttpResponse(response_mess,status_code=500)

        
