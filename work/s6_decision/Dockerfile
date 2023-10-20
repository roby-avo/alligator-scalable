FROM python:3.9-slim
WORKDIR /code
COPY . .
RUN pip install orjson
CMD ["python", "export.py", "./output.json"]
