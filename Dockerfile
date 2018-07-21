FROM python:3.6.6

ENV PYTHONUNBUFFERED 1

RUN mkdir /app
# Requirements are installed here to ensure they will be cached.
COPY ./requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

WORKDIR /app

COPY . /app

CMD ["python",  "/app/server.py"]
