# unbound-perf

Throughput of an HTTP/3 `CONNECT` tunnel with and without
[unbound data](https://datatracker.ietf.org/doc/draft-rosomakho-httpbis-h3-unbound-data/),
measured separately for [quinn](https://github.com/quinn-rs/quinn) and
[s2n-quic](https://github.com/aws/s2n-quic).

```
cargo run --release                       # the whole sweep, both implementations
cargo run --release -- --help             # all the knobs
cargo run --release -- --libs quinn --chunks 512 --total-mib 256
```

## What it measures

A client opens a `CONNECT` tunnel to a server over loopback and pushes a fixed payload through
it one chunk at a time. Each combination is measured three ways:

| mode | on the wire | in the data path |
| --- | --- | --- |
| `framed` | every chunk wrapped in a `DATA` frame | h3 |
| `unbound` | `UNBOUND_DATA`, then raw bytes | h3 |
| `handoff` | `UNBOUND_DATA`, then raw bytes | the QUIC implementation itself |

The third mode is what `into_unbound()` is for: once both ends stop framing, h3 has nothing left
to do on the stream, so it hands it back and the application talks to
`quinn::SendStream`/`s2n_quic::stream::SendStream` directly. Comparing `framed` with `unbound`
isolates what the framing costs; comparing `unbound` with `handoff` isolates what h3's own
send/receive path costs on top.

Reading the table:

- **`DATA headers`** is what framing costs on the wire for that chunk size — purely arithmetic,
  no measurement involved. It is tiny except for very small chunks.
- **`noise`** is how much the same measurement moved between iterations. Deltas smaller than the
  noise on that row mean nothing. Longer runs (`--total-mib`) reduce it.

## How it is set up, and what that implies

- **One thread.** The runtime is `current_thread`, and client and server share it. A run
  therefore measures the *total* protocol work per byte rather than how well two cores overlap,
  which is what makes the modes comparable. It also means the absolute MiB/s figures are not a
  throughput claim for either library — only the differences between modes are the result.
- **Medians, not best-of.** Throughput here jitters by several percent, and reporting the fastest
  iteration would hand the win to whichever cell got lucky. An earlier version did exactly that
  and produced an 8% "regression" that did not exist.
- **All three modes share one connection, interleaved round-robin.** The mode travels in a
  request header, so each round runs framed, unbound and handoff back to back. This matters:
  throughput on this machine drifts over tens of seconds, and measuring one mode after another
  would charge that drift to whichever ran last.
- **Warmup rounds are discarded.** The first two or three transfers on a fresh connection are
  measurably slower — s2n-quic ramps from ~71 to ~86 MiB/s and then holds steady — so
  `--warmup 3` runs before the clock counts.
- **One-way, client to server**, which is what a tunnel upload looks like. Each side switches
  only the direction it uses — the client sends `UNBOUND_DATA`, the server only receives it — so
  `handoff` hands over a single half on each end (`into_unbound_send`, `into_unbound_recv`).
- **The same API shape in every mode.** The `handoff` mode writes owned `Bytes` chunks
  (`quinn::SendStream::write_chunk`, `s2n_quic::stream::SendStream::send`) and reads owned chunks
  (`read_chunk`, `receive`), which is exactly what the h3 backends do internally. No mode pays
  for a copy another one avoids.
- **Generous flow-control windows** on both implementations (32 MiB per stream), so a run is
  never window-bound.
- The measurement loop itself lives in `src/tunnel.rs` and is generic over `h3::quic::Connection`,
  so both libraries execute the same code.

Use `--show-runs` to print every timed iteration; it is the quickest way to tell a real delta
from a ramp or an outlier.

Cross-implementation runs are deliberately not supported; interop is a correctness question, not
a performance one, and mixing stacks would make the numbers harder to attribute.

## What the numbers look like

Measured on an Apple M4 Max (macOS 27.0), 128 MiB per run, median of 5 after 3 warmup rounds.
Your machine will differ, but the shape should not:

```
quinn — 128 MiB per run, median of 5 (3 warmup)

     chunk          framed         unbound         handoff   unbound Δ   handoff Δ  DATA headers   noise
      64 B       6.9 MiB/s      19.4 MiB/s      19.0 MiB/s     +178.7%     +173.8%         4.69%   ±6.2%
     512 B     111.2 MiB/s     181.0 MiB/s     185.0 MiB/s      +62.8%      +66.3%         0.59%   ±4.7%
     4 KiB     222.2 MiB/s     232.4 MiB/s     239.1 MiB/s       +4.6%       +7.6%         0.07%   ±5.0%
    16 KiB     235.2 MiB/s     238.5 MiB/s     243.2 MiB/s       +1.4%       +3.4%         0.03%   ±4.8%
    64 KiB     240.1 MiB/s     240.5 MiB/s     244.6 MiB/s       +0.2%       +1.9%         0.01%   ±9.8%

s2n-quic — 128 MiB per run, median of 5 (3 warmup)

     chunk          framed         unbound         handoff   unbound Δ   handoff Δ  DATA headers   noise
      64 B      43.6 MiB/s      70.4 MiB/s      73.7 MiB/s      +61.3%      +68.9%         4.69%  ±24.8%
     512 B     123.9 MiB/s     139.3 MiB/s     143.4 MiB/s      +12.4%      +15.7%         0.59%   ±1.1%
     4 KiB     148.7 MiB/s     151.9 MiB/s     153.3 MiB/s       +2.2%       +3.1%         0.07%   ±1.9%
    16 KiB     153.0 MiB/s     154.3 MiB/s     154.4 MiB/s       +0.8%       +0.9%         0.03%   ±4.0%
    64 KiB     153.9 MiB/s     154.1 MiB/s     154.9 MiB/s       +0.1%       +0.6%         0.01%   ±1.4%
```

The gain falls away monotonically as chunks grow, in both implementations, and neither trend is
about the bytes saved on the wire:

1. **Small chunks win big** — far more than the 4.69% of header bytes at 64 B can explain. The
   reason is that a `DATA` frame is written as *two* pieces: h3 hands the frame header and the
   payload to the transport separately, so a framed chunk costs two writes into the QUIC stream
   where an unbound chunk costs one. At 64 B the per-write cost dominates everything else.
2. **Large chunks are a wash.** At 16 KiB and above the framing is 0.03% of the bytes and the
   per-write overhead is amortised, so all three modes sit at whatever the loopback path can
   sustain. If a tunnel moves data in large chunks, unbound data buys little; if it relays small
   writes — interactive sessions, telemetry, anything latency-shaped — it is a large win.

The gap between `unbound` and `handoff` is h3's own send/receive path, with framing already out
of the picture. It is consistently small, so most of the win is the framing rather than h3's
overhead — handing the stream back is worth it mainly for what it lets the application do next
(`copy_bidirectional` straight onto the QUIC stream, backend-specific knobs, no h3 in the loop).

Watch the `noise` column before reading anything into a small delta: the s2n-quic 64 B row above
carries ±24.8%, all of it one outlier iteration, which is why the median rather than the best run
is reported.

## Dependencies

The h3 and s2n-quic forks carrying unbound data support are pulled straight from git:

```toml
h3        = { git = "https://github.com/yaroslavros/h3", branch = "unframed-connect" }
h3-quinn  = { git = "https://github.com/yaroslavros/h3", branch = "unframed-connect" }
s2n-quic-h3 = { git = "https://github.com/yaroslavros/s2n-quic", branch = "unframed-connect" }
```

`s2n-quic-h3` still depends on the published `h3`, so `[patch.crates-io]` redirects that to the
same branch. To work against local checkouts instead, point both the dependencies and the patch
at paths:

```toml
[patch.crates-io]
h3 = { path = "../h3/h3" }
```

`certs/` holds the self-signed test certificate vendored from `s2n-quic-core`, which both
s2n-tls and rustls accept. It is a demo certificate — never use it for anything else.
