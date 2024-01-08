import logging
from seleniumbase import Driver
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from seleniumbase import Driver
from bs4 import BeautifulSoup
import json
import sys
import re
from sys import path
import os
from datetime import datetime
import os

sys.stdin.reconfigure(encoding='utf-8')
sys.stdout.reconfigure(encoding='utf-8')

from adls_module import ADLSModule

###################### Utility Function ######################
def extract_page_in_url(url):
    return url.rsplit("/", 1)[-1]

def get_current_time_str():
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%d_%m_%Y_%H_%M")
    return formatted_datetime




def createChromeDriver(num_chrome):
    chrome_drivers = []
    for _ in range(num_chrome):
        driver = Driver(uc_cdp=True, incognito=True,block_images=True,headless=True)
        chrome_drivers.append(driver)
    return chrome_drivers

def extract_property_urls_single_page(page_url,html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    base_url='https://batdongsan.com.vn'
    property_urls=[base_url+element.get('href') for element in soup.select('.js__product-link-for-product-id')]
    return property_urls


###################### Multi-threaded Seleniumbase Scrapping Function ######################
def process_single_property(property_url,chrome_driver):
    # print(property_url)
    chrome_driver.get(property_url)
    html_content = chrome_driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')
    
    elements =  soup.find('div', class_='re__pr-specs-content js__other-info')
    titles=elements.find_all('span', class_='re__pr-specs-content-item-title')
    titles=[title.get_text() for title in titles]
    values=elements.find_all('span', class_='re__pr-specs-content-item-value')
    values=[value.get_text() for value in values]
    property_attribute=dict(zip(titles, values))
    order_attribbute={}

    all_attributes = [
        "Diện tích","Mức giá",
        "Mặt tiền","Đường vào",
        "Hướng nhà","Hướng ban công",
        "Số tầng","Số phòng ngủ",
        "Số toilet","Pháp lý","Nội thất"    
    ]
    all_attributes = {key: None for key in all_attributes}

    for attr in all_attributes:
        if(attr not in property_attribute):
            order_attribbute[attr]=None
        else:
            order_attribbute[attr]=property_attribute[attr]
    order_attribbute['property_id']=re.search(r'\d+$', property_url).group()


    return order_attribbute


def process_single_page(page_url,chrome_driver,adls,max_retry=1):
    # print(page_url)
    # chrome_driver.get(page_url)
    properties=[]
    limit=3
    num_prop=1
    for attempt in range(max_retry + 1):
        try:
            chrome_driver.get(page_url)

            # Check for Cloudflare bot detection
            if "Cloudflare" in chrome_driver.page_source:
                raise Exception("Cloudflare detected in the page source")
            
            html_content = chrome_driver.page_source
            property_urls = extract_property_urls_single_page(page_url, html_content)
            for property_url in property_urls:
                properties.append(process_single_property(property_url,chrome_driver))
                num_prop+=1
                if(num_prop>limit):
                    file_name=f"scraped_data/{get_current_time_str()}_{extract_page_in_url(page_url)}.json"
                    file_content=json.dumps(properties, ensure_ascii=False,indent=4)
                    # print(file_name)
                    # print(file_content)
                    
                    # Write to local file
                    # with open(file_name, 'w',encoding='utf-8') as json_file:
                    #     json_file.write(file_content)

                    # Write to Data Lake 
                    if adls is not None:
                        adls.upload_file_to_container('bronze',file_content,file_name)

                    print(file_name)

                    return properties
        except Exception as e:
            print(f"Attempt {attempt + 1}: Error - {e}")
            sleep(2)
    
    print(f"All attempts failed. Returning None for {page_url}")
    return []

    
def process_multiple_pages(id_range, chrome_driver, store,adls=None):
    if store is None:
        store = []
    for url in id_range:
        store.append(process_single_page(url,chrome_driver,adls))
    return store

def threaded_selenium_scrapping(nthreads, id_range,adls=None):
    store = []
    threads = []
    chrome_drivers=createChromeDriver(nthreads)
    for idx, chrome_driver in enumerate(chrome_drivers):
        ids = id_range[idx::nthreads]
        print(ids)
        t = Thread(target=process_multiple_pages, args=(ids,chrome_driver,store,adls))
        threads.append(t)

    # start the threads
    [ t.start() for t in threads ]
    # wait for the threads to finish
    [ t.join() for t in threads ]
    print(len(store))

    # with open(f"reconciled_data/f{get_current_time_str()}_reconciled_properies.json", 'w',encoding='utf-8') as json_file:
    #     json.dump(store, json_file, ensure_ascii=False, indent=4)

    for cd in chrome_drivers:
        cd.quit()


