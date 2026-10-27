#!/bin/sh
# Runs the framed measurement against the unmodified upstream libraries, then the full sweep
# against the forked ones, with whatever arguments you pass to both.
#
# The two `framed` columns answer "did the unbound data work slow down plain CONNECT?"; the rest
# of the forked table answers "what does unbound data buy?".
#
#   ./compare.sh                                   # defaults
#   ./compare.sh --iters 10 --chunks 512,65536     # same arguments for both builds
set -e

cargo build --release --quiet
(cd baseline && cargo build --release --quiet)

echo "########## upstream baseline — unmodified h3, h3-quinn, s2n-quic, s2n-quic-h3 ##########"
./baseline/target/release/unbound-perf-baseline "$@"

echo
echo "########## forked libraries — unbound data support ##########"
./target/release/unbound-perf "$@"
