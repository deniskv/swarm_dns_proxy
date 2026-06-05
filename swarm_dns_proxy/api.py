from __future__ import annotations

import logging
import aiohttp
from aiohttp import web

from swarm_dns_proxy.config import Config
from swarm_dns_proxy.dns_server import DnsServer

logger = logging.getLogger("swarm-dns-proxy")


class HttpApi:
    def __init__(self, dns_server: DnsServer, config: Config):
        self.dns = dns_server
        self.config = config
        self._runner: web.AppRunner | None = None

    async def start(self, port: int = 8053):
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
        return web.json_response({"status": "ok"})

    async def _records(self, request):
        snapshot = self.dns.get_snapshot()
        flat = []
        for fqdn, ips in sorted(snapshot.items()):
            for ip in ips:
                flat.append({"fqdn": fqdn, "ip": ip})
        return web.json_response({"records": flat, "count": len(flat)})
