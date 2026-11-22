FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates chromium curl fonts-liberation nginx novnc openbox \
      websockify x11-utils x11vnc xvfb \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir playwright==1.64.1 \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin avtopro \
    && install -d -o avtopro -g avtopro -m 0700 /data \
    && install -d /etc/chromium/policies/managed \
    && echo '{"PasswordManagerEnabled":false,"BrowserSignin":0,"DefaultNotificationsSetting":2}' \
       > /etc/chromium/policies/managed/avtopro.json

WORKDIR /opt/avtopro
COPY integrations/avtopro-browser/start.sh integrations/avtopro-browser/nginx.conf integrations/avtopro-browser/worker.py ./
COPY app/avtopro_extension/capture.js ./
COPY app/avtopro_status.py ./
ENV PYTHONUNBUFFERED=1 DISPLAY=:99

RUN install -d -m 1777 /tmp/.X11-unix
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/bin/bash", "/opt/avtopro/start.sh"]
