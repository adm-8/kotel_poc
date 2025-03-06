FROM apache/flink:1.19.0-scala_2.12

RUN wget -P /opt/sql-client/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar
RUN wget -P /opt/sql-client/lib https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar
RUN wget -P /opt/sql-client/lib https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
RUN wget -P /opt/sql-client/lib https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar
RUN wget -P /opt/sql-client/lib https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.19.0/flink-json-1.19.0.jar

# Install Python and pip
RUN apt-get update && apt-get install -y python3.10 python3-pip

# Set Python 3.10 as the default for `python` and `pip`
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1 \
    && update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

# Install PyFlink
RUN pip install apache-flink==1.19.0

WORKDIR /app

COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

COPY ./app /app

CMD ["python3", "process_offer_nomination.py"]
