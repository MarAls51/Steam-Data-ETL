FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y cron postgresql postgresql-contrib sudo && \
    apt-get clean

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN echo "30 2 * * * root python /app/main.py --update-games >> /var/log/cron.log 2>&1" > /etc/cron.d/steam-etl-cron && \
    chmod 0644 /etc/cron.d/steam-etl-cron && \
    crontab /etc/cron.d/steam-etl-cron

RUN touch /var/log/cron.log

COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 5432

CMD ["/app/start.sh"]
