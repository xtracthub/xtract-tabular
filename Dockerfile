FROM ubuntu:latest

FROM python:3.6

MAINTAINER Tyler J. Skluzacek (skluzacek@uchicago.edu)

COPY xtract_tabular_main.py requirements.txt /
COPY tests /tests

ENV CONTAINER_VERSION=1.0

RUN pip install -r requirements.txt
