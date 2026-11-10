import os
import shutil
from pathlib import Path

import pytest
from pyln.testing.fixtures import *  # noqa: F401,F403

PLUGIN = Path(__file__).parent.parent / "target" / "debug" / "xrebalance"
SUBSCRIBER = Path(__file__).parent / "plugins" / "part_subscriber.py"
HOLD = Path(__file__).parent / "plugins" / "hold_htlcs.py"


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
            "or extend PATH (v26.04+ required)", returncode=1)


@pytest.fixture
def xrebalance_plugin():
    if not PLUGIN.exists():
        pytest.fail(f"{PLUGIN} missing; run `cargo build` first")
    # Stale-binary guard: a global cargo target-dir redirect makes
    # `cargo build` land in the shared store while these tests read
    # THIS path — a silently stale plugin turns every assertion into a
    # test of yesterday's code (observed 2026-11-10: the review-fix
    # suite ran green against a binary predating the fix).
    src_dir = PLUGIN.parents[2] / "src"
    newest_src = max(p.stat().st_mtime for p in src_dir.glob("*.rs"))
    if PLUGIN.stat().st_mtime < newest_src:
        pytest.fail(
            f"{PLUGIN} is older than src/*.rs — stale binary. Build with "
            f"CARGO_TARGET_DIR pointing at the repo (or copy the fresh "
            f"build from the shared store) and re-run.")
    return str(PLUGIN)


@pytest.fixture
def part_subscriber():
    return str(SUBSCRIBER)


@pytest.fixture
def hold_htlcs():
    return str(HOLD)


@pytest.fixture
def plugin_opts(xrebalance_plugin, part_subscriber):
    """Options for the node hosting the plugin.  The regtest asks are
    100_000 msat, below the fragment floor's default, so the floor is
    lowered here to keep planning intact; test_min_part_floor sets
    its own values."""
    return {'plugin': [xrebalance_plugin, part_subscriber],
            'xrebalance-min-part-msat': 10_000}
