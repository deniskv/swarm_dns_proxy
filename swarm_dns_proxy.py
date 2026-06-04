#!/usr/bin/env python3
"""
Swarm DNS Proxy — dynamic DNS for Docker Swarm service replicas.

Watches Docker Swarm services via the Docker socket and maintains
DNS A-records whose FQDNs follow a user-defined template.

Supported network endpoint modes: vip, dnsrr, host.

Template variables:
  {node}          — hostname of the Swarm node running the task
  {service}       — service name
  {stack}         — stack name (from com.docker.stack.namespace label, or "default")
  {slot}          — task slot number (1-based for replicated services)
  {task_id}       — short (12-char) task ID
  {replica}       — zero-padded slot, e.g. 001, 002 …
  {network}       — attachment network name

Example template:
  {node}.replica-{replica}.{service}.swarm.local
  →  worker-1.replica-001.nginx.swarm.local
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import signal
import struct
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
import yaml

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Docker Swarm Watcher
# ---------------------------------------------------------------------------

@dataclass
class ReplicaRecord:
    fqdn: str
    ip: str
    node: str
    service: str
    task_id: str
    slot: int
    network: str
    endpoint_mode: str  # vip | dnsrr | host


class SwarmWatcher:
    """Polls Docker Swarm API via unix socket, resolves replica IPs."""

    def __init__(self, config: Config):
        self.config = config
        self._connector: aiohttp.UnixConnector | None = None
        self._session: aiohttp.ClientSession | None = None

    async def start(self):
        self._connector = aiohttp.UnixConnector(path=self.config.docker_socket)
        self._session = aiohttp.ClientSession(connector=self._connector)

    async def stop(self):
        if self._session:
            await self._session.close()

    async def _get(self, path: str) -> Any:
        assert self._session
        url = f"http://localhost{path}"
        async with self._session.get(url) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.error("Docker API %s -> %d: %s", path, resp.status, text[:200])
                return None
            return await resp.json()

    async def collect_records(self) -> list[ReplicaRecord]:
        """Main collection: services → tasks → nodes → network attachments."""
        services = await self._get("/services") or []
        tasks = await self._get("/tasks?filters=" + json.dumps({"desired-state": ["running"]})) or []
        nodes_raw = await self._get("/nodes") or []
        networks_raw = await self._get("/networks") or []

        node_map: dict[str, str] = {}  # node_id -> hostname
        for n in nodes_raw:
            node_map[n["ID"]] = n.get("Description", {}).get("Hostname", n["ID"][:12])

        network_map: dict[str, str] = {}  # network_id -> name
        for net in networks_raw:
            network_map[net["Id"]] = net.get("Name", net["Id"][:12])

        svc_map: dict[str, dict] = {}  # service_id -> service object
        for svc in services:
            svc_map[svc["ID"]] = svc

        # Build filter set if configured
        svc_filter: dict[str, ServiceFilter] | None = None
        if self.config.services:
            svc_filter = {sf.name: sf for sf in self.config.services}

        records: list[ReplicaRecord] = []

        for task in tasks:
            if task.get("Status", {}).get("State") != "running":
                continue

            svc_id = task.get("ServiceID", "")
            svc = svc_map.get(svc_id)
            if not svc:
                continue

            svc_name = svc.get("Spec", {}).get("Name", svc_id[:12])

            # Apply service filter
            svc_override: ServiceFilter | None = None
            if svc_filter is not None:
                svc_override = svc_filter.get(svc_name)
                if svc_override is None:
                    continue  # skip services not in filter

            stack = svc.get("Spec", {}).get("Labels", {}).get(
                "com.docker.stack.namespace", "default"
            )

            node_id = task.get("NodeID", "")
            node_hostname = node_map.get(node_id, node_id[:12])

            slot = task.get("Slot", 0)
            task_id_short = task["ID"][:12]

            # Determine endpoint mode
            endpoint_spec = svc.get("Spec", {}).get("EndpointSpec", {})
            endpoint_mode = endpoint_spec.get("Mode", "vip")  # vip | dnsrr

            # Check if service uses host mode for any port
            ports = svc.get("Endpoint", {}).get("Ports", [])
            uses_host_mode = any(
                p.get("PublishMode") == "host" for p in ports
            )

            # Collect IPs from task's network attachments
            net_attachments = task.get("NetworksAttachments", [])
            if not net_attachments:
                # For host-mode-only services, use node IP or skip
                if uses_host_mode:
                    # Try to get node address
                    node_obj = next(
                        (n for n in nodes_raw if n["ID"] == node_id), None
                    )
                    node_addr = ""
                    if node_obj:
                        node_addr = (
                            node_obj.get("Status", {}).get("Addr", "")
                            or node_obj.get("ManagerStatus", {}).get("Addr", "").split(":")[0]
                        )
                    if node_addr:
                        tmpl = (svc_override and svc_override.template) or self.config.template
                        fqdn = self._render_template(
                            tmpl, node_hostname, svc_name, stack,
                            slot, task_id_short, "host"
                        )
                        records.append(ReplicaRecord(
                            fqdn=fqdn, ip=node_addr, node=node_hostname,
                            service=svc_name, task_id=task_id_short,
                            slot=slot, network="host",
                            endpoint_mode="host",
                        ))
                continue

            for att in net_attachments:
                net_id = att.get("Network", {}).get("ID", "")
                net_name = network_map.get(net_id, net_id[:12])

                # Skip the ingress network — it's internal to Swarm routing
                if net_name == "ingress":
                    continue

                # Apply network filter if set
                if svc_override and svc_override.networks:
                    if net_name not in svc_override.networks:
                        continue

                addresses = att.get("Addresses", [])
                for addr in addresses:
                    ip = addr.split("/")[0]  # strip CIDR

                    eff_mode = "host" if uses_host_mode else endpoint_mode

                    tmpl = (svc_override and svc_override.template) or self.config.template
                    fqdn = self._render_template(
                        tmpl, node_hostname, svc_name, stack,
                        slot, task_id_short, net_name,
                    )
                    records.append(ReplicaRecord(
                        fqdn=fqdn, ip=ip, node=node_hostname,
                        service=svc_name, task_id=task_id_short,
                        slot=slot, network=net_name,
                        endpoint_mode=eff_mode,
                    ))

        return records

    def _render_template(
        self, tmpl: str, node: str, service: str, stack: str,
        slot: int, task_id: str, network: str,
    ) -> str:
        replica = str(slot).zfill(3)
        fqdn = tmpl.format(
            node=node,
            service=service,
            stack=stack,
            slot=slot,
            task_id=task_id,
            replica=replica,
            network=network,
        )
        if self.config.domain_suffix and not fqdn.endswith(self.config.domain_suffix):
            fqdn = fqdn.rstrip(".") + "." + self.config.domain_suffix.lstrip(".")
        return fqdn.lower()


# ---------------------------------------------------------------------------
# Minimal DNS Server (UDP, A-record only)
# ---------------------------------------------------------------------------

DNS_TYPE_A = 1
DNS_CLASS_IN = 1
DNS_RCODE_OK = 0
DNS_RCODE_NXDOMAIN = 3
DNS_RCODE_SERVFAIL = 2


def encode_dns_name(name: str) -> bytes:
    parts = name.rstrip(".").split(".")
    result = b""
    for part in parts:
        encoded = part.encode("ascii")
        result += bytes([len(encoded)]) + encoded
    result += b"\x00"
    return result


def decode_dns_name(data: bytes, offset: int) -> tuple[str, int]:
    labels = []
    jumped = False
    max_offset = offset
    visited = set()
    jumps = 0
    while True:
        if offset >= len(data):
            break
        if offset in visited or jumps > 10:
            raise ValueError("Malformed packet: compression loop or excessive pointer jumps")
        visited.add(offset)
        length = data[offset]
        if length == 0:
            offset += 1
            if not jumped:
                max_offset = offset
            break
        if (length & 0xC0) == 0xC0:
            if not jumped:
                max_offset = offset + 2
            pointer = struct.unpack("!H", data[offset:offset + 2])[0] & 0x3FFF
            offset = pointer
            jumped = True
            jumps += 1
            continue
        offset += 1
        labels.append(data[offset:offset + length].decode("ascii", errors="replace"))
        offset += length
        if not jumped:
            max_offset = offset
    return ".".join(labels), max_offset


def build_dns_response(
    query: bytes,
    answers: list[tuple[str, str, int]],  # [(name, ip, ttl), ...]
    rcode: int = DNS_RCODE_OK,
) -> bytes:
    """Build a minimal DNS response for A-record queries."""
    if len(query) < 12:
        return b""
    txn_id = query[:2]
    flags = struct.pack("!H", 0x8000 | (rcode & 0xF))  # QR=1, Opcode=0, AA=1
    flags = struct.pack("!H", 0x8400 | (rcode & 0xF))  # QR=1, AA=1, RA=0
    qd_count = struct.unpack("!H", query[4:6])[0]
    an_count = struct.pack("!H", len(answers))
    ns_count = struct.pack("!H", 0)
    ar_count = struct.pack("!H", 0)

    header = txn_id + flags + struct.pack("!H", qd_count) + an_count + ns_count + ar_count

    # Copy question section
    offset = 12
    for _ in range(qd_count):
        _, offset = decode_dns_name(query, offset)
        offset += 4  # QTYPE + QCLASS

    question = query[12:offset]

    # Build answer section
    answer_section = b""
    for name, ip, ttl in answers:
        answer_section += encode_dns_name(name)
        answer_section += struct.pack("!HHI", DNS_TYPE_A, DNS_CLASS_IN, ttl)
        ip_parts = [int(x) for x in ip.split(".")]
        answer_section += struct.pack("!H", 4)  # RDLENGTH
        answer_section += bytes(ip_parts)

    return header + question + answer_section


class DnsServer:
    """Async UDP DNS server that resolves from the record table."""

    def __init__(self, config: Config):
        self.config = config
        self._records: dict[str, list[str]] = {}  # fqdn -> [ip, ...]
        self._lock = asyncio.Lock()
        self._transport: asyncio.DatagramTransport | None = None

    async def update_records(self, replicas: list[ReplicaRecord]):
        new_records: dict[str, list[str]] = {}
        for r in replicas:
            key = r.fqdn.rstrip(".").lower()
            new_records.setdefault(key, []).append(r.ip)
        async with self._lock:
            if new_records != self._records:
                added = set(new_records) - set(self._records)
                removed = set(self._records) - set(new_records)
                if added:
                    logger.info("DNS records added: %s", ", ".join(sorted(added)))
                if removed:
                    logger.info("DNS records removed: %s", ", ".join(sorted(removed)))
                self._records = new_records

    def get_snapshot(self) -> dict[str, list[str]]:
        return dict(self._records)

    async def start(self):
        loop = asyncio.get_running_loop()

        class _Protocol(asyncio.DatagramProtocol):
            def __init__(self, server: DnsServer):
                self.server = server

            def connection_made(self, transport):
                self.server._transport = transport

            def datagram_received(self, data, addr):
                asyncio.ensure_future(self.server._handle_query(data, addr))

        transport, _ = await loop.create_datagram_endpoint(
            lambda: _Protocol(self),
            local_addr=("0.0.0.0", self.config.dns_port),
        )
        logger.info("DNS server listening on UDP :%d", self.config.dns_port)

    async def _handle_query(self, data: bytes, addr: tuple):
        if len(data) < 12:
            return
        try:
            qname, offset = decode_dns_name(data, 12)
            qtype = struct.unpack("!H", data[offset:offset + 2])[0]
            qclass = struct.unpack("!H", data[offset + 2:offset + 4])[0]
        except Exception:
            logger.debug("Malformed DNS query from %s", addr)
            return

        qname_lower = qname.rstrip(".").lower()

        if qtype == DNS_TYPE_A and qclass == DNS_CLASS_IN:
            async with self._lock:
                ips = self._records.get(qname_lower)

            if ips is not None:
                answers = [(qname, ip, self.config.ttl) for ip in ips]
                response = build_dns_response(data, answers)
                logger.debug("Resolved %s -> %s", qname, ips)
                if self._transport:
                    self._transport.sendto(response, addr)
                return

        # Forward to upstream
        await self._forward_upstream(data, addr)

    async def _forward_upstream(self, data: bytes, client_addr: tuple):
        """Forward unmatched queries to upstream DNS."""
        loop = asyncio.get_running_loop()
        try:
            fut = loop.create_future()

            class _Upstream(asyncio.DatagramProtocol):
                def __init__(self):
                    self.transport = None

                def connection_made(self, transport):
                    self.transport = transport
                    transport.sendto(data)

                def datagram_received(self, resp_data, addr):
                    if not fut.done():
                        fut.set_result(resp_data)

                def error_received(self, exc):
                    if not fut.done():
                        fut.set_exception(exc)

            transport, _ = await loop.create_datagram_endpoint(
                _Upstream,
                remote_addr=(self.config.upstream_dns, self.config.upstream_port),
            )
            try:
                response = await asyncio.wait_for(fut, timeout=5.0)
                if self._transport:
                    self._transport.sendto(response, client_addr)
            except asyncio.TimeoutError:
                logger.warning("Upstream DNS timeout for query from %s", client_addr)
            finally:
                transport.close()
        except Exception as e:
            logger.error("Upstream forward error: %s", e)


# ---------------------------------------------------------------------------
# HTTP API — lightweight status / health endpoint
# ---------------------------------------------------------------------------

class HttpApi:
    def __init__(self, dns_server: DnsServer, config: Config):
        self.dns = dns_server
        self.config = config
        self._runner: aiohttp.web.AppRunner | None = None

    async def start(self, port: int = 8053):
        from aiohttp import web

        app = web.Application()
        app.router.add_get("/health", self._health)
        app.router.add_get("/records", self._records)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", port)
        await site.start()
        logger.info("HTTP API listening on :%d", port)

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    async def _health(self, request):
        from aiohttp import web
        return web.json_response({"status": "ok"})

    async def _records(self, request):
        from aiohttp import web
        snapshot = self.dns.get_snapshot()
        flat = []
        for fqdn, ips in sorted(snapshot.items()):
            for ip in ips:
                flat.append({"fqdn": fqdn, "ip": ip})
        return web.json_response({"records": flat, "count": len(flat)})


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run(config: Config):
    watcher = SwarmWatcher(config)
    dns = DnsServer(config)
    http = HttpApi(dns, config)

    await watcher.start()
    await dns.start()

    http_port = int(os.environ.get("HTTP_PORT", "8053"))
    await http.start(http_port)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    logger.info(
        "Swarm DNS Proxy started — template=%s, poll=%ds",
        config.template, config.poll_interval,
    )

    while not stop_event.is_set():
        try:
            records = await watcher.collect_records()
            await dns.update_records(records)
            if records:
                logger.debug("Active records: %d", len(records))
        except Exception as e:
            logger.error("Poll error: %s", e)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=config.poll_interval)
        except asyncio.TimeoutError:
            pass

    logger.info("Shutting down …")
    await http.stop()
    await watcher.stop()


def main():
    config = Config.from_env()
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
