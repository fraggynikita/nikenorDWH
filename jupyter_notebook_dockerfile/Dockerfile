FROM jupyter/base-notebook:latest

COPY requirements.txt /tmp/

USER root

RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN apt update && apt install -y openjdk-17-jdk