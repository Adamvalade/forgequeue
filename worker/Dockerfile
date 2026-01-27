FROM python:3.12-slim

WORKDIR /app

COPY worker/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY common /app/common
COPY worker/worker.py /app/worker.py

CMD ["python", "worker.py"]
