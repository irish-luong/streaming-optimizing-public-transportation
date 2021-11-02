FROM python:3.7

RUN mkdir -p /app
COPY . /app
WORKDIR /app

USER root

RUN chmod 777 /app/entrypoint.sh

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install -r producers/requirements.txt
RUN pip install -r consumers/requirements.txt
RUN export PYTHONPATH=/app