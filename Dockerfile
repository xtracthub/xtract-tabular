FROM ubuntu:latest

FROM python:3.6

MAINTAINER Ryan Wong

COPY xtract_tabular_main.py requirements.txt /
COPY tests /tests

RUN pip install -r requirements.txt
RUN pip install git+https://github.com/Parsl/parsl
RUN pip install git+https://github.com/DLHub-Argonne/home_run

#ENTRYPOINT ["python", "xtract_tabular_main.py"]
