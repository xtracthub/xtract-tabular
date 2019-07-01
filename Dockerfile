FROM python:latest

MAINTAINER Ryan Wong

COPY main_structured.py /

RUN pip install pandas argparse

CMD ["python", "main_structured.py"]