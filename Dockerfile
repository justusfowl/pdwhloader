FROM python:3

RUN apt-get update -y

# adding custom MS repository
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

# install SQL Server drivers
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN apt-get install -y tdsodbc unixodbc-dev

RUN wget http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.0.0_1.0.2g-1ubuntu4_amd64.deb  \
    &&  dpkg -i libssl1.0.0_1.0.2g-1ubuntu4_amd64.deb

ADD odbcinst.ini /etc/odbcinst.ini

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD python API.py
