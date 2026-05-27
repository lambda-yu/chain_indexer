from __future__ import annotations

import json
import zlib
from typing import Any

import httpx
import structlog
from solders.pubkey import Pubkey

log = structlog.get_logger(__name__)

_IDL_SEED = b"anchor:idl"
_ANCHOR_DISCRIMINATOR_LEN = 8


def _idl_address(program_id: str) -> str:
    program_pubkey = Pubkey.from_string(program_id)
    idl_pubkey, _ = Pubkey.find_program_address(
        [_IDL_SEED, bytes(program_pubkey)],
        program_pubkey,
    )
    return str(idl_pubkey)


async def fetch_idl_from_chain(
    rpc_url: str, program_id: str,
) -> dict[str, Any] | None:
    idl_addr = _idl_address(program_id)
    log.info("idl_fetcher.resolving", program_id=program_id, idl_address=idl_addr)

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getAccountInfo",
            "params": [idl_addr, {"encoding": "base64"}],
        })
        result = resp.json().get("result")
        if result is None or result.get("value") is None:
            log.warning("idl_fetcher.account_not_found", idl_address=idl_addr)
            return None

        data_b64 = result["value"]["data"][0]
        import base64
        raw = base64.b64decode(data_b64)

    # Anchor IDL account layout:
    # [8 bytes discriminator] [4 bytes authority_option] [32 bytes authority] [4 bytes data_len LE] [N bytes zlib data]
    # OR newer format: [8 bytes disc] [32 bytes authority] [4 bytes data_len LE] [N bytes zlib data]
    # We skip the header and find the zlib stream by looking for the zlib magic bytes
    offset = _ANCHOR_DISCRIMINATOR_LEN
    idl_json = _try_decompress(raw, offset)
    if idl_json is None:
        log.warning("idl_fetcher.decompress_failed", program_id=program_id)
        return None

    try:
        idl: dict[str, Any] = json.loads(idl_json)
    except json.JSONDecodeError:
        log.warning("idl_fetcher.invalid_json", program_id=program_id)
        return None

    if "metadata" not in idl:
        idl["metadata"] = {}
    if "address" not in idl["metadata"]:
        idl["metadata"]["address"] = program_id

    log.info("idl_fetcher.success", program_id=program_id, events=len(idl.get("events", [])), instructions=len(idl.get("instructions", [])))
    return idl


def _try_decompress(raw: bytes, start: int) -> str | None:
    for offset in range(start, min(start + 80, len(raw))):
        try:
            decompressed = zlib.decompress(raw[offset:])
            return decompressed.decode("utf-8")
        except (zlib.error, UnicodeDecodeError):
            continue
    return None
