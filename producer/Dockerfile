FROM python:3.12-slim

RUN pip install --no-cache-dir kafka-python

WORKDIR /app
COPY producer/producer.py /app/producer.py

CMD ["python", "/app/producer.py"]
