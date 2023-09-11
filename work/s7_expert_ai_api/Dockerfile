FROM python:3.9-slim
WORKDIR /code
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "classifier.py", "./output.json"]
