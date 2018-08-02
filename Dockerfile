# TODO: upgrade to 3.7 once deps update
FROM python:3.6

ENV PYTHONUNBUFFERED 1

COPY ./entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r//' /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN mkdir /app
# Requirements are installed here to ensure they will be cached.
COPY ./requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY ./src/* /app/

WORKDIR /app/

ENTRYPOINT ["/entrypoint.sh"]
