FROM python:3.11-slim

WORKDIR /app

COPY migrate.py .
COPY archive/healthcare_dataset.csv ./data.csv

RUN pip install --no-cache-dir pandas pymongo

CMD ["python", "migrate.py"]
