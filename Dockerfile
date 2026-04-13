FROM python:3.12-slim

LABEL maintainer="swarm-dns-proxy"
LABEL description="Dynamic DNS for Docker Swarm service replicas"

RUN pip install --no-cache-dir aiohttp pyyaml

WORKDIR /app
COPY swarm_dns_proxy.py .
COPY config.yaml /etc/swarm-dns-proxy/config.yaml

EXPOSE 53/udp
EXPOSE 8053/tcp

ENTRYPOINT ["python", "-u", "swarm_dns_proxy.py"]
