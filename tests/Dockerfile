FROM python:3.12-slim

WORKDIR /app

COPY tests/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY tests /app/tests

CMD ["pytest", "-q"]
