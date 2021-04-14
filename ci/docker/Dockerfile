FROM debian:stable

RUN \
   apt update -y && \
   apt install -y wget git

COPY kiara /usr/bin/kiara

RUN \
   chmod +x /usr/bin/kiara
