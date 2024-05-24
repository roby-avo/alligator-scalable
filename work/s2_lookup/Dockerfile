FROM python:3.9-slim
WORKDIR /code
COPY . .
ENV LAMAPI_ENDPOINT=https://lamapi.hel.sintef.cloud/
ENV LAMAPI_TOKEN=lamapi_demo_2023
RUN pip install -r requirements.txt
CMD ["python", "lookup.py", "./output.json"]
