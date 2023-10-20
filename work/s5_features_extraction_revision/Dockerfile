FROM python:3.9-slim
WORKDIR /code
COPY . .
RUN pip install orjson
CMD ["python", "features_extraction_revision.py", "./output.json", "cea"]
