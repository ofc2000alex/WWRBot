FROM python:3.12-bookworm
WORKDIR /wwrbot
COPY . .
RUN pip install -r requirements.txt
