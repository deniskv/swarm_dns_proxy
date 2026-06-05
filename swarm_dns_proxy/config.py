from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from pathlib import Path
import yaml

DEFAULT_CONFIG_PATH = "/etc/swarm-dns-proxy/config.yaml"
DEFAULT_TEMPLATE = "{node}.replica-{replica}.{service}.swarm.local"
DEFAULT_DNS_PORT = 53
DEFAULT_DNS_TTL = 30
DEFAULT_POLL_INTERVAL = 5  # seconds
DEFAULT_DOCKER_SOCKET = "/var/run/docker.sock"
DEFAULT_UPSTREAM_DNS = "8.8.8.8"
DEFAULT_UPSTREAM_PORT = 53

logger = logging.getLogger("swarm-dns-proxy")


@dataclass
class ServiceFilter:
    """Optional per-service overrides."""
    name: str
    template: str | None = None
    networks: list[str] | None = None  # limit to specific networks


@dataclass
class Config:
    template: str = DEFAULT_TEMPLATE
    dns_port: int = DEFAULT_DNS_PORT
    ttl: int = DEFAULT_DNS_TTL
    poll_interval: int = DEFAULT_POLL_INTERVAL
    docker_socket: str = DEFAULT_DOCKER_SOCKET
    upstream_dns: str = DEFAULT_UPSTREAM_DNS
    upstream_port: int = DEFAULT_UPSTREAM_PORT
    domain_suffix: str = ""
    services: list[ServiceFilter] = field(default_factory=list)
    log_level: str = "INFO"

    @classmethod
    def from_file(cls, path: str) -> "Config":
        p = Path(path)
        if not p.exists():
            logger.warning("Config %s not found, using defaults", path)
            return cls()
        raw = yaml.safe_load(p.read_text()) or {}
        svcs = []
        for s in raw.get("services", []):
            svcs.append(ServiceFilter(
                name=s["name"],
                template=s.get("template"),
                networks=s.get("networks"),
            ))
        return cls(
            template=raw.get("template", DEFAULT_TEMPLATE),
            dns_port=int(raw.get("dns_port", DEFAULT_DNS_PORT)),
            ttl=int(raw.get("ttl", DEFAULT_DNS_TTL)),
            poll_interval=int(raw.get("poll_interval", DEFAULT_POLL_INTERVAL)),
            docker_socket=raw.get("docker_socket", DEFAULT_DOCKER_SOCKET),
            upstream_dns=raw.get("upstream_dns", DEFAULT_UPSTREAM_DNS),
            upstream_port=int(raw.get("upstream_port", DEFAULT_UPSTREAM_PORT)),
            domain_suffix=raw.get("domain_suffix", ""),
            services=svcs,
            log_level=raw.get("log_level", "INFO"),
        )

    @classmethod
    def from_env(cls) -> "Config":
        """Override file config with environment variables."""
        path = os.environ.get("CONFIG_PATH", DEFAULT_CONFIG_PATH)
        cfg = cls.from_file(path)
        cfg.template = os.environ.get("DNS_TEMPLATE", cfg.template)
        cfg.dns_port = int(os.environ.get("DNS_PORT", cfg.dns_port))
        cfg.ttl = int(os.environ.get("DNS_TTL", cfg.ttl))
        cfg.poll_interval = int(os.environ.get("POLL_INTERVAL", cfg.poll_interval))
        cfg.docker_socket = os.environ.get("DOCKER_SOCKET", cfg.docker_socket)
        cfg.upstream_dns = os.environ.get("UPSTREAM_DNS", cfg.upstream_dns)
        cfg.upstream_port = int(os.environ.get("UPSTREAM_PORT", cfg.upstream_port))
        cfg.log_level = os.environ.get("LOG_LEVEL", cfg.log_level)
        return cfg
