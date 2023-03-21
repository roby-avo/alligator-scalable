FROM python:3.9-slim
WORKDIR /code
COPY . .
ENV LAMAPI_ENDPOINT=149.132.176.50:8097
ENV LAMAPI_TOKEN=insideslab-lamapi-2022
RUN pip install -r requirements.txt
