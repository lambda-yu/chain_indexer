"""E2E: Solana catchup uses get_blocks against solana-test-validator.

Placeholder. tests/e2e/conftest.py has no `solana_validator` fixture today.
Implementing this test requires:

1. Add a fixture that spawns solana-test-validator (subprocess), exposes its
   RPC URL, and tears down on session end. Pattern after the `anvil` fixture.
2. Produce engineered skipped slots (solana-test-validator does not skip
   slots deterministically; may need a custom config or post-hoc mock).
3. Wire ChainRunner against the validator with slot_query_range_blocks=10.
4. Assert get_blocks call count + only-valid-slots fetch_block pattern,
   same shape as tests/unit/test_solana_catchup_range.py.

Correctness of the optimization is already proven by:
  - tests/unit/test_solana_get_blocks.py (get_blocks adapter call)
  - tests/unit/test_solana_catchup_range.py (catchup window + skip semantics)
"""
from __future__ import annotations

import pytest


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_catchup_skips_empty_slots():
    pytest.skip("e2e harness — needs solana_validator fixture in tests/e2e/conftest.py")
