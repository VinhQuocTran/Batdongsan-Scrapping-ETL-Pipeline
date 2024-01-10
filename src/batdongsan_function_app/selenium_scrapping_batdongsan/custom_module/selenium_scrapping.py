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

from .adls_module import ADLSModule

###################### Utility Function ######################
def extract_page_in_url(url):
    return url.rsplit("/", 1)[-1]

def get_current_time_str():
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%d_%m_%Y_%H_%M")
    return formatted_datetime

def extract_coordinates(html_content):
    # Define a regular expression pattern to extract coordinates
    pattern = r'place\?q=([-+]?\d*\.\d+),([-+]?\d*\.\d+)'

    # Use re.search to find the first match in the HTML content
    match = re.search(pattern, html_content)

    # Check if a match is found
    if match:
        # Extract latitude and longitude from the matched groups
        latitude = float(match.group(1))
        longitude = float(match.group(2))
        return [latitude, longitude]
    else:
        return [None,None]


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
    
    # Find property info
    elements =  soup.find('div', class_='re__pr-specs-content js__other-info')
    titles=elements.find_all('span', class_='re__pr-specs-content-item-title')
    titles=[title.get_text() for title in titles]
    values=elements.find_all('span', class_='re__pr-specs-content-item-value')
    values=[value.get_text() for value in values]

    # Find property address
    address= soup.find('span', class_='re__pr-short-description js__pr-address').text
    titles.append("Địa chỉ")
    values.append(address)

    # Find property map coordination
    map_coor=soup.find('div', class_='re__section re__pr-map js__section js__li-other')
    map_coor=extract_coordinates(str(map_coor))
    titles.append("latitude")
    titles.append("longtitude")
    values.append(map_coor[0])
    values.append(map_coor[1])
    
    # Find date info
    short_info=soup.find('div', class_='re__pr-short-info re__pr-config js__pr-config')
    short_info_titles=short_info.find_all('span', class_='title')
    short_info_titles=[title.get_text() for title in short_info_titles]
    short_info_values=short_info.find_all('span', class_='value')
    short_info_values=[value.get_text() for value in short_info_values]

    # Merge 2 list
    titles.extend(short_info_titles)
    values.extend(short_info_values)


    property_attribute=dict(zip(titles, values))
    order_attribbute={}

    all_attributes = [
        "Diện tích","Mức giá",
        "Mặt tiền","Đường vào",
        "Hướng nhà","Hướng ban công",
        "Số tầng","Số phòng ngủ",
        "Số toilet","Pháp lý","Nội thất",
        'Ngày đăng', 'Ngày hết hạn', 
        'Loại tin', 'Mã tin', 'Địa chỉ',
        "latitude","longtitude"
    ]
    all_attributes = {key: None for key in all_attributes}

    for attr in all_attributes:
        if(attr not in property_attribute):
            order_attribbute[attr]=None
        else:
            order_attribbute[attr]=property_attribute[attr]

    return order_attribbute


def process_single_page(page_url,chrome_driver,limit_each_page,adls,max_retry=1):
    print(f"The process's scrapping {page_url}...")
    properties=[]
    for attempt in range(max_retry + 1):
        try:
            chrome_driver.get(page_url)

            # Check for Cloudflare bot detection
            if "Cloudflare" in chrome_driver.page_source:
                raise Exception("Cloudflare detected in the page source")
            
            # Scrapping data for each property
            html_content = chrome_driver.page_source
            property_urls = extract_property_urls_single_page(page_url, html_content)
            for idx,property_url in zip(range(limit_each_page),property_urls):
                properties.append(process_single_property(property_url,chrome_driver))

            # Write to ADLS and local
            file_name=f"scraped_data/{get_current_time_str()}_{extract_page_in_url(page_url)}.json"
            file_content=json.dumps(properties, ensure_ascii=False,indent=4)

            with open(file_name, 'w',encoding='utf-8') as json_file:
                json_file.write(file_content)

            if adls is not None:
                adls.upload_file_to_container('bronze',file_content,file_name)
            
            print(f"Scrapping {page_url} done.")
            return properties
        except Exception as e:
            print(f"Attempt {attempt + 1}: Error - {e}, try again in 5s")
            sleep(5)
    
    print(f"All attempts failed. Returning None for {page_url}")
    return []

    
def process_multiple_pages(id_range, chrome_driver, store,limit_each_page,adls=None):
    if store is None:
        store = []
    for url in id_range:
        store.append(process_single_page(url,chrome_driver,limit_each_page,adls))
    return store

def threaded_selenium_scrapping(nthreads,id_range,limit_each_page,adls=None):
    store = []
    threads = []
    chrome_drivers=createChromeDriver(nthreads)
    for idx, chrome_driver in enumerate(chrome_drivers):
        ids = id_range[idx::nthreads]
        print(ids)
        t = Thread(target=process_multiple_pages, args=(ids,chrome_driver,store,limit_each_page,adls))
        threads.append(t)

    # start the threads
    [ t.start() for t in threads ]
    # wait for the threads to finish
    [ t.join() for t in threads ]

    with open(f"reconciled_data/{get_current_time_str()}_reconciled_properies.json", 'w',encoding='utf-8') as json_file:
        json.dump(store, json_file, ensure_ascii=False, indent=4)

    for cd in chrome_drivers:
        cd.quit()


