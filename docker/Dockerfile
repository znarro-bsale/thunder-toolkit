FROM alpine:latest
RUN apk add --update \
    python3 \
    py3-pip \
    ruby \
    ruby-dev \
    build-base \
  && rm -rf /var/cache/apk/*

WORKDIR /app
COPY . /app
RUN pip3 install -r requirements.txt
ARG ENV
ENV env=$ENV
CMD ["python3", "main.py"]
