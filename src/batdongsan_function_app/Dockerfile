#=================================
# Base images
#=================================
FROM mcr.microsoft.com/azure-functions/python:4-python3.9



#=================================
# Install Bash Command Line Tools
#=================================
RUN apt-get update \
  && apt-get -qy --no-install-recommends install \
    sudo \
    unzip \
    wget \
    curl \
    libxi6 \
    libgconf-2-4 \
    xvfb \
  && rm -rf /var/lib/apt/lists/*



#=================================
# Install dependencies
#=================================
COPY requirements.txt /
RUN pip install -r /requirements.txt




#================
# Install Chrome
#================
RUN curl -sS -o - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list && \
    apt-get -yqq update && \
    apt-get -yqq install google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

RUN seleniumbase get uc_driver
RUN seleniumbase get chromedriver




#=======================================
# Copy Azure Function
#=======================================
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AZUREFUNCTIONSJOBHOST__LOGGING__CONSOLE__ISENABLED=true

COPY . /home/site/wwwroot
ARG CONFIG_JSON
COPY ${CONFIG_JSON} /home/site/wwwroot