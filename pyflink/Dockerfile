FROM apache/flink:1.18.0-scala_2.12

# Install Python and pip
RUN apt-get update && apt-get install -y python3.10 python3-pip

# Set Python 3.10 as the default for `python` and `pip`
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1 \
    && update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

# Install PyFlink
RUN pip install apache-flink==1.18.0

WORKDIR /app

COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

COPY ./app /app

CMD ["python3", "process_offer_nomination.py"]
