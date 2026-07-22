#!/bin/sh
# Build the release binary and install it under a git-described
# versioned name, then point a stable symlink at it:
#
#   $bindir/xrebalance-v0.1.0-3-gabc1234
#   $bindir/xrebalance -> xrebalance-v0.1.0-3-gabc1234
#
# Reverting is re-pointing the symlink; the running version is
# visible in the filename, and lightningd resolves the symlink so
# the plugin's log prefix carries it too.
#
# Overrides (environment):
#   XREBALANCE_BINDIR             install directory (/usr/local/bin)
#   XREBALANCE_VERSIONED_SYMLINK  symlink name (xrebalance)
#   XREBALANCE_VERSIONED_DESCRIBE version string (git describe)
set -e

bindir="${XREBALANCE_BINDIR:-/usr/local/bin}"
symlink="${XREBALANCE_VERSIONED_SYMLINK:-xrebalance}"
describe="${XREBALANCE_VERSIONED_DESCRIBE:-}"

cd "$(dirname "$0")"

if [ -z "$describe" ] \
   && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
	describe="$(git describe --tags --always --dirty)"
fi
case "$describe" in
v*) ;;
*)
	# No tag reachable (or not a git tree): synthesize the same
	# shape from the crate version.
	version="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml \
		   | head -n1)"
	describe="v$version${describe:+-g$describe}"
	;;
esac

cargo build --release --locked

mkdir -p "$bindir"
install -m 0755 target/release/xrebalance "$bindir/xrebalance-$describe"
ln -sf "xrebalance-$describe" "$bindir/$symlink"
echo "Installed $bindir/xrebalance-$describe and set $bindir/$symlink"
