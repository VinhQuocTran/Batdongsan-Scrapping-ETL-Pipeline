# [Real Estate Scrapping](https://batdongsan.com.vn/) Data Pipeline
<!-- Start Document Outline -->

<!-- End Document Outline -->
## Problem and Objective
- During a research process for building  [Tokenized Real Estate Trading Exchange](https://github.com/VinhQuocTran/Finalterm-Real-Estate-Blockchain) in my Blockchain university class, I stumbled on an article and it really caught my attention because the title [Purchasing a house in Vietnam is an uphill battle - VnExpress International](https://e.vnexpress.net/news/readers-views/purchasing-a-house-in-vietnam-is-an-uphill-battle-4547223.html). The article states that 
> **"The average income of Vietnamese is just over $3,000 a year. Thus, it will take us more than <u>160 years-worth of income to purchase a house.</u>"**
- As a "most rational Reddit user", I rarely believe in any newspaper unless it is backed by number, otherwise I will have to gather data and do due diligence for myself. 
- So I decided to take this as a learning opportunity for building web scrapping pipeline and get data ready for analysis process.
## Architecture
The pipeline crawls data from website [Batdongsan by PropertyGuru](https://batdongsan.com.vn/) and consists of various modules and technologies
- **Microsoft Azure**: Azure Functions (crawl, extract and load data), Azure Data Lake Storage Gen2 (store raw/transformed data)
- **Docker**: Containerize your code and deploy the image to Azure Functions for auto-scaling crawler
- **ADLS Module**: my custom class contains common function to help you interact with data in ADLS Gen2
- **Spark Structure Streaming**: a data processing framework help you to unify both of your batch and streaming pipeline without rewriting your code  
- **Databricks**: a big data platform that allows me run Spark Structure Streaming without setting up infrastructure on my own.
- **PowerBI**: BI tool to help connect and get data from Delta Table using Power Query and create dashboard for analysis
- **Prefect**: An orchestration tool
![Batdongsan architecture diagram](png/Batdongsan-architecture-diagram.svg)
### Overview
- The [Batdongsan by PropertyGuru](https://batdongsan.com.vn/)  multi-threaded crawler is written in Azure Functions and can be containerized to deploy as an image to Azure for auto-scaling
- The extracted data from the crawler is directly stored to  containers in Azure Data Lake Storage (ADLS). **The whole data pipeline starts running when you trigger the HTTP of your `selenium_scrapping_batdongsan` Azure Function**
### ETL Flow
- You trigger HTTP of `selenium_scrapping_batdongsan` Azure function to start a pipeline. The crawler scrapes all properties's information in each page, all needed parameters are: 
    - **nthreads**: set number of threads do you want, each thread is a Selenium Chrome browser (i.e: If you set 3 threads then you have 3 Selenium Chrome browser scrapping concurrently)
    - **num_pages** determines how many pages you want to scrape
    - **limit_each_page** determines how many properties you want to scrape in each page (max and default is 20)
    - **init_url_format**: 
- Raw data saved as json file and uploaded to `Bronze` container in ADLS Gen2
![json format](png/json_format.png)
- After the scrapping process finished, the `bds_main_function` notebook in Databricks will ingest data from bronze container and create **Bronze Delta Table**. Then data is transformed/deduplicated and load to **Silver Delta Table**
- Finally, Using Power Query from PowerBI to directly load data from Delta Table and create dashboard
![delta table power query](png/delta_table_power_query.png)

### PowerBI Dashboard
![bgk dashboard](png/bgk_dashboard.png)
## How to run
### Prerequisites
Install the modules belows
- **Docker Desktop and Docker Engine** for running container
- **VS Code and extensions** for locally debug and run Azure functions: Azure Account, Azure Function, Azure Resources
- **Postman** for sending API request

### Setting up Azure resources
Create all resources below to prepare for the data pipeline
- 1 Azure Data Lake Storage account Gen 2 (ADLS) and 2 containers for our data layer: bronze, silver
![data layer](png/data_layer.png)

### Create `config.json` in `src/batdongsan_function_app` path  and put your credentials to access Azure resources
- If you want to upload raw data to ADLS containers, you have to set up your credentials like ADLS connection string, key, name
![adls credentials](png/adls_credentials.png)

### Azure Functions Overview
Every Azure function folder will have the structure like the image below
- `__init__.py` contains main code to execute
- `function.json` contains a function's settings include **binding**/**trigger**
![azure function structure](png/azure_function_structure.png)

### Test and debug our data pipeline
Follow the path `src/py/` inside repo to open `boardgamegeek_fa` folder using VS code, then press F5 and run Postman to test the pipeline
![postman test function](png/postman_test_function.png)
=> After you've done with debug and testing, you can deploy the whole Azure Function project into cloud using **VScode** and run from there

