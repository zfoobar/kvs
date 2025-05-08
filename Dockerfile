FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends 

COPY . /app

RUN apt-get update && apt-get install -y bash

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 9888

ENV PYTHONPATH=/app/kvs:$PYTHONPATH

CMD ["python", "main.py"]
