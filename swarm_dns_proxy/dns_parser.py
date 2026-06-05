import struct

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
    query_flags = struct.unpack("!H", query[2:4])[0]
    rd_flag = query_flags & 0x0100  # Extract Recursion Desired (RD) bit
    flags = struct.pack("!H", 0x8400 | rd_flag | (rcode & 0xF))  # QR=1, AA=1, preserve RD, set RCODE
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
