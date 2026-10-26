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
  throughput can drift over tens of seconds, and measuring one mode after another would charge
  that drift to whichever ran last.
- **Warmup rounds are discarded.** The first two or three transfers on a fresh connection are
  measurably slower — throughput ramps and then holds steady — so `--warmup 3` runs before the
  clock counts.
- **One-way, client to server**, which is what a tunnel upload looks like. Each side switches
  only the direction it uses — the client sends `UNBOUND_DATA`, the server only receives it — so
  `handoff` hands over a single half on each end (`into_unbound_send`, `into_unbound_recv`).
- **The `handoff` mode uses the API an application would reach for**: owned `Bytes` per write
  (`quinn::SendStream::write_chunk`, `s2n_quic::stream::SendStream::send`) and owned chunks per
  read (`read_chunk`, `receive`). h3's own send path cannot do that — it passes the payload to the
  transport as a borrowed slice, which costs a copy — and that difference is a real part of what
  the handoff saves, not a handicap imposed by this harness. Equalising it by making `handoff`
  copy too would measure something nobody writes. See finding 2 below.
- **Generous flow-control windows** on both implementations (32 MiB per stream), so a run is
  never window-bound.
- The measurement loop itself lives in `src/tunnel.rs` and is generic over `h3::quic::Connection`,
  so both libraries execute the same code.

Use `--show-runs` to print every timed iteration; it is the quickest way to tell a real delta
from a ramp or an outlier.

Cross-implementation runs are deliberately not supported; interop is a correctness question, not
a performance one, and mixing stacks would make the numbers harder to attribute.

## What the numbers look like

Measured on an i9-13900K running Linux, 128 MiB per run, median of 10 after 3 warmup rounds
(`--iters 10`):

```
quinn — 128 MiB per run, median of 10 (3 warmup)

     chunk          framed         unbound         handoff   unbound Δ   handoff Δ  DATA headers   noise
      64 B       8.6 MiB/s      30.7 MiB/s      32.8 MiB/s     +257.1%     +281.7%         4.69%  ±21.0%
     512 B     207.5 MiB/s     501.9 MiB/s     535.1 MiB/s     +141.9%     +157.9%         0.59%  ±16.3%
     4 KiB     822.8 MiB/s     923.4 MiB/s    1044.3 MiB/s      +12.2%      +26.9%         0.07%   ±1.3%
    16 KiB     952.6 MiB/s     979.4 MiB/s    1102.2 MiB/s       +2.8%      +15.7%         0.03%   ±1.3%
    64 KiB     980.2 MiB/s     987.8 MiB/s    1113.8 MiB/s       +0.8%      +13.6%         0.01%   ±1.8%

s2n-quic — 128 MiB per run, median of 10 (3 warmup)

     chunk          framed         unbound         handoff   unbound Δ   handoff Δ  DATA headers   noise
      64 B      93.5 MiB/s     235.6 MiB/s     296.0 MiB/s     +152.1%     +216.7%         4.69%   ±2.5%
     512 B     494.5 MiB/s     756.6 MiB/s     857.3 MiB/s      +53.0%      +73.3%         0.59%   ±0.7%
     4 KiB     979.8 MiB/s    1074.1 MiB/s    1130.4 MiB/s       +9.6%      +15.4%         0.07%   ±0.8%
    16 KiB    1111.1 MiB/s    1147.5 MiB/s    1188.2 MiB/s       +3.3%       +6.9%         0.03%   ±1.1%
    64 KiB    1156.0 MiB/s    1168.6 MiB/s    1205.6 MiB/s       +1.1%       +4.3%         0.01%   ±0.9%
```

Both implementations tell the same story, and the two columns of deltas answer two different
questions.

**1. Dropping the framing is what wins on small writes.** At 64 B the gain is 257% (quinn) and
152% (s2n-quic) — far more than the 4.69% of header bytes can explain. A `DATA` frame is written
as *two* pieces: h3 hands the frame header and the payload to the transport separately, so a
framed chunk costs two writes into the QUIC stream where an unbound chunk costs one. At 64 B the
per-write cost dominates everything else. The gain then falls away as chunks grow and that cost
is amortised: by 64 KiB, `unbound` is only +0.8% / +1.1% over `framed`.

**2. Handing the stream back keeps winning at every chunk size** — +13.6% (quinn) and +4.3%
(s2n-quic) at 64 KiB, against ±1.8% / ±0.9% noise. That is not about framing, which is already
gone in both modes being compared. It is a per-byte cost in h3's send path: h3 passes the payload
to the transport as a *borrowed slice*, so the transport has to copy it, while the native API
takes an owned `Bytes` and does not. In quinn-proto the two paths are visible side by side —
`ByteSlice::pop_chunk` does `Bytes::from(self.data[..limit].to_owned())`, a full copy, whereas
`BytesArray::pop_chunk` (reached via `write_chunk`) does `chunk.split_to(limit)`, a refcount
split. s2n-quic-h3 copies in the same place, with a `TODO` pointing at
[hyperium/h3#78](https://github.com/hyperium/h3/issues/78).

So the handoff's advantage at large chunks is really the cost of an h3 API limitation, which a
zero-copy send path in h3 would narrow. Until then, taking the stream back is worth a few percent
of CPU on every byte, on top of whatever the framing costs.

**3. Watch the `noise` column.** The small-chunk rows are the noisy ones (quinn at 64 B and 512 B
still move ±16-21% even over 10 iterations), which is ironic given they carry the largest effects
— the effects are so large that the noise does not threaten them. The rows where a few percent
*is* the whole result, 4 KiB and up, are the stable ones at ±1-2%.

### The platform matters more than you would think

An earlier run of the same code on an Apple M4 Max (macOS 27.0) capped out near 240 MiB/s in
*every* mode, and the handoff gain at 64 KiB shrank to +1.9% (quinn) and +0.6% (s2n-quic) — which
reads as "no difference at large chunks". macOS gives neither library the UDP batching and
segmentation offload they use on Linux, so the transport path there is expensive enough to hide
h3's per-byte cost entirely: the bottleneck was never h3.

That is worth keeping in mind when reading any result from this harness. It measures the protocol
work per byte against whatever the local transport path costs; make the transport cheap, as Linux
does, and h3's share of the work becomes visible. The Linux numbers above are the ones to use for
a server deployment.

Comparing the two tables against each other is not the point — the implementations have different
defaults and the harness only equalises flow-control windows. Each table's delta columns are the
result.

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
