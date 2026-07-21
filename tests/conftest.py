import os
import shutil
from pathlib import Path

import pytest
from pyln.testing.fixtures import *  # noqa: F401,F403

PLUGIN = Path(__file__).parent.parent / "target" / "debug" / "xrebalance"
SUBSCRIBER = Path(__file__).parent / "plugins" / "part_subscriber.py"


def pytest_configure(config):
    """pyln-testing launches `lightningd` from PATH.  Accept
    LIGHTNINGD=/path/to/lightningd as a convenience, and fail with a
    clear message rather than one FileNotFoundError per fixture."""
    lightningd = os.environ.get("LIGHTNINGD")
    if lightningd:
        os.environ["PATH"] = os.pathsep.join(
            [str(Path(lightningd).parent), os.environ["PATH"]])
    if shutil.which("lightningd") is None:
        pytest.exit(
            "no lightningd on PATH; set LIGHTNINGD=/path/to/lightningd "
            "or extend PATH (v26.06+ required)", returncode=1)


@pytest.fixture
def xrebalance_plugin():
    if not PLUGIN.exists():
        pytest.fail(f"{PLUGIN} missing; run `cargo build` first")
    return str(PLUGIN)


@pytest.fixture
def part_subscriber():
    return str(SUBSCRIBER)
