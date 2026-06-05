from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

from swarm_dns_proxy.config import Config
from swarm_dns_proxy.watcher import SwarmWatcher
from swarm_dns_proxy.dns_server import DnsServer
from swarm_dns_proxy.api import HttpApi

logger = logging.getLogger("swarm-dns-proxy")


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
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
    except NotImplementedError:
        # loop.add_signal_handler is not implemented on Windows.
        # Fall back to standard keyboard interrupt handling.
        pass

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
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
