FROM ubuntu:latest

FROM python:3.6

MAINTAINER Tyler J. Skluzacek (skluzacek@uchicago.edu)

COPY xtract_tabular_main.py requirements.txt /
COPY tests /tests

RUN pip install -r requirements.txt
