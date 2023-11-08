FROM python:3.9-slim
WORKDIR /code
COPY . .
ENV ANALYZER_ENDPOINT="https://deployenv6.expertcustomers.ai:8084/services/emd/analyze"
RUN pip install -r requirements.txt
