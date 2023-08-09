FROM python:3.9-slim
WORKDIR /code
COPY . .
ENV LAMAPI_ENDPOINT=https://lamapi.hel.sintef.cloud/
ENV LAMAPI_TOKEN=SINTEF2023!
RUN pip install -r requirements.txt
CMD ["python", "features_extraction.py", "./output.json"]
