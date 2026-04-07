FROM andrius/asterisk:latest
WORKDIR /etc/asterisk
USER root
RUN cat /etc/os-release && \
    apt update && \
    apt install asterisk-core-sounds-en asterisk-moh-opsound-wav iproute2 vim telnet inetutils-ping -y 
