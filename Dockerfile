FROM python:latest

MAINTAINER Ryan Wong

COPY xtract_tabular_main.py /

RUN pip install pandas argparse

ENTRYPOINT ["python", "xtract_tabular_main.py"]