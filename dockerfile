FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y cron && \
    apt-get clean

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN echo "30 2 * * * root python /app/main.py --update-games >> /var/log/cron.log 2>&1" > /etc/cron.d/steam-etl-cron && \
    chmod 0644 /etc/cron.d/steam-etl-cron && \
    crontab /etc/cron.d/steam-etl-cron

RUN touch /var/log/cron.log

CMD cron && tail -f /var/log/cron.log
