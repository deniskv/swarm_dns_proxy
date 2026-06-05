from __future__ import annotations

import asyncio
import logging
import struct

from swarm_dns_proxy.config import Config
from swarm_dns_proxy.dns_parser import (
    DNS_TYPE_A,
    DNS_CLASS_IN,
    DNS_RCODE_OK,
    DNS_RCODE_NXDOMAIN,
    DNS_RCODE_SERVFAIL,
    decode_dns_name,
    build_dns_response,
)
from swarm_dns_proxy.watcher import ReplicaRecord

logger = logging.getLogger("swarm-dns-proxy")


class DnsServer:
    """Async UDP DNS server that resolves from the record table."""

    def __init__(self, config: Config):
        self.config = config
        self._records: dict[str, list[str]] = {}  # fqdn -> [ip, ...]
        self._transport: asyncio.DatagramTransport | None = None
        self._semaphore: asyncio.Semaphore | None = None

    async def update_records(self, replicas: list[ReplicaRecord]):
        new_records: dict[str, list[str]] = {}
        for r in replicas:
            key = r.fqdn.rstrip(".").lower()
            new_records.setdefault(key, []).append(r.ip)
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

    async def _handle_query_bounded(self, data: bytes, addr: tuple):
        assert self._semaphore is not None
        async with self._semaphore:
            await self._handle_query(data, addr)

    async def start(self):
        loop = asyncio.get_running_loop()
        self._semaphore = asyncio.Semaphore(1000)

        class _Protocol(asyncio.DatagramProtocol):
            def __init__(self, server: DnsServer):
                self.server = server

            def connection_made(self, transport):
                self.server._transport = transport

            def datagram_received(self, data, addr):
                if self.server._semaphore.locked():
                    return
                asyncio.create_task(self.server._handle_query_bounded(data, addr))

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

        # Determine if query is in our managed zone
        suffix = self.config.domain_suffix.strip(".").lower()
        if not suffix:
            last_brace = self.config.template.rfind("}")
            if last_brace != -1:
                suffix = self.config.template[last_brace + 1:].strip(".").lower()

        is_local_zone = False
        if suffix and (qname_lower == suffix or qname_lower.endswith("." + suffix)):
            is_local_zone = True

        exists = qname_lower in self._records

        if exists:
            if qtype == DNS_TYPE_A and qclass == DNS_CLASS_IN:
                ips = self._records[qname_lower]
                answers = [(qname, ip, self.config.ttl) for ip in ips]
                response = build_dns_response(data, answers, rcode=DNS_RCODE_OK)
                logger.debug("Resolved %s -> %s", qname, ips)
                if self._transport:
                    self._transport.sendto(response, addr)
                return
            else:
                # Return empty NOERROR response for other query types of managed records
                response = build_dns_response(data, [], rcode=DNS_RCODE_OK)
                logger.debug("Local match for %s, but unsupported type/class (%d/%d). Returning empty NOERROR.", qname, qtype, qclass)
                if self._transport:
                    self._transport.sendto(response, addr)
                return
        elif is_local_zone:
            # Local zone match but record doesn't exist -> return local NXDOMAIN
            response = build_dns_response(data, [], rcode=DNS_RCODE_NXDOMAIN)
            logger.debug("NXDOMAIN for local zone query %s", qname)
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
            finally:
                transport.close()
        except asyncio.TimeoutError:
            logger.warning("Upstream DNS timeout for query from %s", client_addr)
            if self._transport:
                fail_resp = build_dns_response(data, [], rcode=DNS_RCODE_SERVFAIL)
                self._transport.sendto(fail_resp, client_addr)
        except Exception as e:
            logger.error("Upstream forward error: %s", e)
            if self._transport:
                try:
                    fail_resp = build_dns_response(data, [], rcode=DNS_RCODE_SERVFAIL)
                    self._transport.sendto(fail_resp, client_addr)
                except Exception:
                    pass
