FROM python:3.12.2-alpine3.19
WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt
ARG cron=consolidate_report_data_cron
ENV env=production
ENV use_proxies=False
ENV cron=$cron
CMD ["python3", "cron.py"]