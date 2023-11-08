FROM python:3.9-slim
WORKDIR /code
COPY . .
ENV CLASSIFIER_ENDPOINT="https://deployenv6.expertcustomers.ai:8084/services/emd/classify"
RUN pip install -r requirements.txt
CMD ["python", "classifier.py", "./output.json"]
