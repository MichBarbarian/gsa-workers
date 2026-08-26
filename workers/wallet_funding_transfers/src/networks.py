"""EVM chain metadata for wallet_funding_transfers."""

from __future__ import annotations

from typing import Any

CHAINS: dict[int, dict[str, Any]] = {
    1: {"slug": "ethereum", "group": "etherscan", "asset": "ETH"},
    42161: {"slug": "arbitrum", "group": "etherscan", "asset": "ETH"},
    137: {"slug": "polygon", "group": "etherscan", "asset": "POL"},
    42220: {"slug": "celo", "group": "etherscan", "asset": "CELO"},
    8453: {"slug": "base", "group": "blockscout", "asset": "ETH"},
    100: {"slug": "gnosis", "group": "blockscout", "asset": "XDAI"},
    56: {"slug": "bsc", "group": "bsc", "asset": "BNB"},
    196: {"slug": "xlayer", "group": "xlayer", "asset": "OKB"},
}

GROUP_EVM_IDS: dict[str, tuple[int, ...]] = {
    "etherscan": (1, 42161, 137, 42220),
    "blockscout": (8453, 100),
    "bsc": (56,),
    "xlayer": (196,),
}
