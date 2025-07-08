FROM python:3.10-slim

#RUN apt-get update && apt-get install -y gcc libmysqlclient-devhr

WORKDIR /app

COPY app/ /app
COPY .env /app/.env
COPY app/requirements.txt /app/requirements.txt

RUN pip install --upgrade pip &&\
    pip install -r requirements.txt

ENV DOCKER_HOST=unix:///var/run/docker.sock

CMD ["python","connect_db.py", "main_flow.py"]

