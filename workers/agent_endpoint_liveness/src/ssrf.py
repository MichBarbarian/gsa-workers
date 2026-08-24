"""Block probes to loopback / private / link-local / cloud metadata."""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlparse

_BLOCKED_HOSTS = {
    "localhost",
    "metadata.google.internal",
    "metadata.google.com",
}


def _ip_blocked(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return bool(
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def ssrf_block_reason(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return "ssrf_blocked:empty_host"
    if host in _BLOCKED_HOSTS:
        return "ssrf_blocked"
    if host.endswith(".localhost") or host.endswith(".internal"):
        return "ssrf_blocked"

    try:
        ip = ipaddress.ip_address(host)
        if _ip_blocked(ip):
            return "ssrf_blocked"
        return None
    except ValueError:
        pass

    try:
        infos = socket.getaddrinfo(host, parsed.port or None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return None

    for info in infos:
        addr = info[4][0]
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            continue
        if _ip_blocked(ip):
            return "ssrf_blocked"
    return None
