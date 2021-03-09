FROM ubuntu:latest

FROM python:3.6

MAINTAINER Ryan Wong

COPY xtract_tabular_main.py requirements.txt /
COPY tests /tests

RUN pip install -r requirements.txt
RUN pip install datasketch
RUN pip install xtract-sdk
RUN pip uninstall parsl
RUN pip install parsl==0.9.0

RUN pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
RUN pip install xtract_sdk==0.0.5

#ENTRYPOINT ["python", "xtract_tabular_main.py"]
