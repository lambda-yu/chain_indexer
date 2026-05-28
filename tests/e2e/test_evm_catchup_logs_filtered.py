"""E2E: catchup uses RPC-side filtering and range queries against anvil.

This is a placeholder. The existing e2e harness (tests/e2e/conftest.py)
provides `anvil` and `webhook_receiver` fixtures, but does not yet expose a
deploy-ERC-20 helper. Implementing the assertion below requires:

1. Deploy two ERC-20 contracts via web3.py (foundry-style bytecode in fixture).
2. Emit Transfer events on both across ~30 blocks.
3. Configure a single token_transfer subscription on contract A only.
4. Start a ChainRunner with log_query_range_blocks=10 from block 0.
5. Wait for catchup to drain.
6. Assert webhook received exactly A's transfer count.
7. Wrap EvmAdapter.fetch_logs with a counter to assert call count ==
   ceil(blocks / 10) (proves range query is used; baseline would be N calls).

Until the harness gets the ERC-20 deploy helper, this test is skipped.
The correctness of the optimization is already proven by:
  - tests/unit/test_evm_catchup_range.py (range query semantics)
  - tests/unit/test_evm_fetch_logs_topics.py (topics param threading)
  - tests/unit/test_evm_fetch_logs_degrade.py (bisection degradation)
  - tests/unit/test_filter_set.py (filter construction logic)
"""
from __future__ import annotations

import pytest


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_catchup_with_address_and_topic_filter():
    pytest.skip("e2e harness — needs ERC-20 deploy helper in tests/e2e/conftest.py")
