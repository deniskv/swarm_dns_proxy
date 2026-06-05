FROM python:3.12-slim

LABEL maintainer="swarm-dns-proxy"
LABEL description="Dynamic DNS for Docker Swarm service replicas"

RUN pip install --no-cache-dir aiohttp==3.9.5 PyYAML==6.0.1

WORKDIR /app
COPY swarm_dns_proxy/ ./swarm_dns_proxy/
COPY config.yaml /etc/swarm-dns-proxy/config.yaml

EXPOSE 53/udp
EXPOSE 8053/tcp

ENTRYPOINT ["python", "-u", "-m", "swarm_dns_proxy"]
