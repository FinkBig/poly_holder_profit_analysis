"""Environment variable loading for API keys (if needed in future)."""

import os
from dotenv import load_dotenv

load_dotenv()


class Keys:
    """Container for API keys and secrets."""

    # No API keys required for public Polymarket endpoints
    # Structure kept for future expansion
    pass
