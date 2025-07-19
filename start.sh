#!/bin/bash
set -e

service postgresql start

sleep 3

sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='testuser'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE USER testuser WITH PASSWORD 'testpass';"

sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw testdb || \
  sudo -u postgres createdb -O testuser testdb

service cron start

tail -f /var/log/cron.log
