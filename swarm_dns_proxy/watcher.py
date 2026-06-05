from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

import aiohttp

from swarm_dns_proxy.config import Config, ServiceFilter, DEFAULT_TEMPLATE

logger = logging.getLogger("swarm-dns-proxy")


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
                resp.raise_for_status()
            return await resp.json()

    async def collect_records(self) -> list[ReplicaRecord]:
        """Main collection: services → tasks → nodes → network attachments."""
        services, tasks, nodes_raw, networks_raw = await asyncio.gather(
            self._get("/services"),
            self._get("/tasks?filters=" + json.dumps({"desired-state": ["running"]})),
            self._get("/nodes"),
            self._get("/networks"),
        )
        services = services or []
        tasks = tasks or []
        nodes_raw = nodes_raw or []
        networks_raw = networks_raw or []

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
        try:
            fqdn = tmpl.format(
                node=node,
                service=service,
                stack=stack,
                slot=slot,
                task_id=task_id,
                replica=replica,
                network=network,
            )
        except (KeyError, ValueError) as e:
            logger.error("Invalid DNS template %r (%s). Falling back to default template.", tmpl, e)
            fqdn = DEFAULT_TEMPLATE.format(
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
