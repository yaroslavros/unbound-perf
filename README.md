# unbound-perf

Throughput of an HTTP/3 `CONNECT` tunnel with and without
[unbound data](https://datatracker.ietf.org/doc/draft-rosomakho-httpbis-h3-unbound-data/),
measured separately for [quinn](https://github.com/quinn-rs/quinn),
[s2n-quic](https://github.com/aws/s2n-quic) and
[quiche](https://github.com/cloudflare/quiche) (through
[tokio-quiche](https://github.com/cloudflare/quiche/tree/master/tokio-quiche)).

```
cargo run --release                       # the whole sweep, all three implementations
cargo run --release -- --help             # all the knobs
cargo run --release -- --libs quinn --chunks 512 --total-mib 256

./compare.sh --iters 10                   # upstream baseline first, then the forked build
```

## What it measures

A client opens a `CONNECT` tunnel to a server over loopback and pushes a fixed payload through
it one chunk at a time. Each combination is measured three ways:

| mode | on the wire | in the data path |
| --- | --- | --- |
| `framed` | every chunk wrapped in a `DATA` frame | the HTTP/3 layer |
| `unbound` | `UNBOUND_DATA`, then raw bytes | the HTTP/3 layer |
| `handoff` | `UNBOUND_DATA`, then raw bytes | the QUIC implementation itself |

The third mode is the point of the handoff: once both ends stop framing, HTTP/3 has nothing left
to do on the stream, so it gets out of the way. Comparing `framed` with `unbound` isolates what
the framing costs; comparing `unbound` with `handoff` isolates what the HTTP/3 layer's own
send/receive path costs on top.

What "gets out of the way" means differs between the two architectures, and that is worth knowing
before reading the `handoff Δ` column:

- **h3 (quinn, s2n-quic)** gives the stream *back*. `into_unbound()` returns the backend's own
  stream, and the application then talks to `quinn::SendStream`/`s2n_quic::stream::SendStream`
  directly. The API changes, and so does the cost per byte — see finding 2 below.
- **quiche (tokio-quiche)** cannot: its `H3Driver` owns the `quiche::Connection`, so no
  application can hold a stream of its own. `OutboundFrame::UnboundData { take_over: true }`
  instead tells the driver to move the bytes straight on the transport, skipping
  `quiche::h3::Connection` entirely. The application keeps using the same channels, so here the
  delta really is only the HTTP/3 layer's own bookkeeping.

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
- **In quiche, no mode pays for a copy the mode it is compared against avoids.** All three go
  through the same owned `Bytes` channel; framed and unbound reach the transport via
  `send_body_zc`, handoff via `stream_send_zc`. The first version of the driver used plain
  `stream_send` on the taken-over stream, which copies out of a borrowed slice, and that was
  enough to make `handoff` measurably *slower* than `unbound` at large chunks. If that row ever
  goes negative again, suspect the same thing.
- **Generous flow-control windows** everywhere (32 MiB per stream), so a run is never
  window-bound.
- **UDP offloads are on for every sender.** quinn and s2n-quic enable GSO themselves on Linux.
  tokio-quiche applies socket options for a *listener*, but a client `Socket` is built with
  every offload off and its own `connect()` deliberately leaves it that way, since not every NIC
  supports GSO — so `src/quiche_backend.rs` calls `apply_max_capabilities()` on the client
  socket. Without it the quiche sender flushes after each packet, one `sendmsg` per ~1350 bytes,
  and the run measures syscall rate instead of protocol work. This is worth re-checking after
  any dependency bump: a sender stuck without GSO shows up as an implausibly flat ceiling across
  every mode and chunk size.
- **quiche still sends smaller datagrams than the other two.** `max_send_udp_payload_size`
  defaults to 1350 against quinn's 1452, which is a few percent of absolute throughput. It is
  left alone: it applies equally to all three modes, so it moves the ceiling, not the deltas.
- The measurement loop for quinn and s2n-quic lives in `src/tunnel.rs` and is generic over
  `h3::quic::Connection`, so those two execute the same code. quiche's HTTP/3 is not generic over
  a transport, so `src/quiche_backend.rs` has its own loop; it is deliberately the same shape —
  one connection per series, modes interleaved round-robin, the clock running from the `CONNECT`
  response to the far end's byte-count report.

Use `--show-runs` to print every timed iteration; it is the quickest way to tell a real delta
from a ramp or an outlier.

Cross-implementation runs are deliberately not supported; interop is a correctness question, not
a performance one, and mixing stacks would make the numbers harder to attribute.

## Checking the fork against upstream

The forked libraries are in the path for *all three* modes, so a regression in one would quietly
shrink the deltas — or invent them. `baseline/` is the control: a second binary that compiles the
same harness source against **unmodified** h3, h3-quinn, s2n-quic, s2n-quic-h3, quiche and
tokio-quiche, pinned to the commits each fork branched from:

```
hyperium/h3        43745ad  webtransport: change accept_bi() to return a resolving stream (#356)
aws/s2n-quic       d20f8fad fix(dc): reduce PSK handshake future size (#3362)
cloudflare/quiche  4ebd2133 quiche: add outstanding sent packets limit (#2839)
```

It only knows the framed mode — the `unbound` cargo feature is off, so the unbound data API is
never referenced and the other two modes are rejected with a message saying why. Compare its
`framed` column against `framed` from the main binary:

```
./compare.sh --iters 10
```

Two details that make this a real control rather than a gesture:

- **The modules are shared, not copied.** `baseline/src/main.rs` pulls in `../../src/*.rs` with
  `#[path]`, so the timing loop, warmup, interleaving, flow-control limits and payload handling
  are the same code. Only the dependency graph differs.
- **One h3 in the graph.** `s2n-quic-h3` asks for `h3` from crates.io, so `[patch.crates-io]`
  sends it to the same pinned upstream commit. Worth confirming after any dependency change:

  ```
  cd baseline && cargo tree -i h3
  h3 v0.0.8 (https://github.com/hyperium/h3?rev=43745ad…)
  ├── h3-quinn v0.0.10 (https://github.com/hyperium/h3?rev=43745ad…)
  └── s2n-quic-h3 v0.1.0 (https://github.com/aws/s2n-quic?rev=d20f8fad…)
  ```

On the macOS machine used while writing this, framed throughput came out the same either way —
+3.0% / −1.2% (quinn, 512 B / 16 KiB), +2.1% / +1.2% (s2n-quic) and +1.9% / −1.5% (quiche), all
inside the ±1–10% noise of those rows. Worth repeating on the machine you publish from, since
that is the platform whose numbers people will read.

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

The quiche table is still to come; it has not been run on this machine yet.

Both implementations above tell the same story, and the two columns of deltas answer two
different questions.

**1. Dropping the framing is what wins on small writes.** At 64 B the gain is 257% (quinn) and
152% (s2n-quic) — far more than the 4.69% of header bytes can explain. A `DATA` frame is written
as *two* pieces: h3 hands the frame header and the payload to the transport separately, so a
framed chunk costs two writes into the QUIC stream where an unbound chunk costs one. At 64 B the
per-write cost dominates everything else. The gain then falls away as chunks grow and that cost
is amortised: by 64 KiB, `unbound` is only +0.8% / +1.1% over `framed`.

**2. Handing the stream back keeps winning at every chunk size, on h3** — +13.6% (quinn) and
+4.3% (s2n-quic) at 64 KiB, against ±1.8% / ±0.9% noise. That is not about framing, which is
already gone in both modes being compared. It is a per-byte cost in h3's send path: h3 passes the
payload to the transport as a *borrowed slice*, so the transport has to copy it, while the native
API takes an owned `Bytes` and does not. In quinn-proto the two paths are visible side by side —
`ByteSlice::pop_chunk` does `Bytes::from(self.data[..limit].to_owned())`, a full copy, whereas
`BytesArray::pop_chunk` (reached via `write_chunk`) does `chunk.split_to(limit)`, a refcount
split. s2n-quic-h3 copies in the same place, with a `TODO` pointing at
[hyperium/h3#78](https://github.com/hyperium/h3/issues/78).

So the handoff's advantage at large chunks is really the cost of an h3 API limitation, which a
zero-copy send path in h3 would narrow. Until then, taking the stream back is worth a few percent
of CPU on every byte, on top of whatever the framing costs.

quiche is the control for that claim. tokio-quiche is already zero-copy on both sides of the
switch — `send_body_zc` and `stream_send_zc` both take the buffer rather than a borrowed slice —
so there is no copy for its handoff to remove, and its `handoff` column should track `unbound`
once the framing is gone. If it does, the h3 gap above is an h3 API property rather than anything
inherent to unbound data. The quiche table will say.

**3. Watch the `noise` column.** The small-chunk rows are the noisy ones (quinn at 64 B and 512 B
still move ±16-21% even over 10 iterations), which is ironic given they carry the largest effects
— the effects are so large that the noise does not threaten them. The rows where a few percent
*is* the whole result, 4 KiB and up, are the stable ones at ±1-2%.

### The platform matters more than you would think

An earlier run of the same code on an Apple M4 Max (macOS 27.0) capped out near 240 MiB/s in
*every* mode, and the handoff gain at 64 KiB shrank to +1.9% (quinn) and +0.6% (s2n-quic) — which
reads as "no difference at large chunks". macOS gives none of these libraries the UDP batching
and segmentation offload they use on Linux, so the transport path there is expensive enough to
hide the HTTP/3 layer's per-byte cost entirely: the bottleneck was never HTTP/3.

That is worth keeping in mind when reading any result from this harness. It measures the protocol
work per byte against whatever the local transport path costs; make the transport cheap, as Linux
does, and h3's share of the work becomes visible. The Linux numbers above are the ones to use for
a server deployment.

Comparing the tables against each other is not the point — the implementations have different
defaults and the harness only equalises flow-control windows and UDP offloads. Each table's delta
columns are the result. In particular, a lower absolute ceiling for one library is far more likely
to be a socket or datagram-size default than anything about its HTTP/3 layer; the first quiche run
capped at ~425 MiB/s against ~980 (quinn) purely because its client socket had no GSO.

## Dependencies

The three forks carrying unbound data support are pulled straight from git:

```toml
h3           = { git = "https://github.com/yaroslavros/h3", branch = "unframed-connect" }
h3-quinn     = { git = "https://github.com/yaroslavros/h3", branch = "unframed-connect" }
s2n-quic-h3  = { git = "https://github.com/yaroslavros/s2n-quic", branch = "unframed-connect" }
quiche       = { git = "https://github.com/yaroslavros/quiche", branch = "unframed-connect" }
tokio-quiche = { git = "https://github.com/yaroslavros/quiche", branch = "unframed-connect" }
```

`s2n-quic-h3` still depends on the published `h3`, so `[patch.crates-io]` redirects that to the
same branch. To work against local checkouts instead, override the git sources on the command
line rather than editing the manifest:

```
cargo build --release \
  --config 'patch."https://github.com/yaroslavros/quiche".quiche.path="../quiche/quiche"' \
  --config 'patch."https://github.com/yaroslavros/quiche".tokio-quiche.path="../quiche/tokio-quiche"'
```

`baseline/Cargo.toml` pins the same crates to their pre-fork upstream commits; see
[Checking the fork against upstream](#checking-the-fork-against-upstream).

**`libc` needs pinning.** quiche caps it at `<0.2.190`, while tokio picks up whatever is newest,
so a fresh resolve fails with "all possible versions conflict". Both lockfiles hold `0.2.189`; if
you regenerate one, re-pin it:

```
cargo update -p libc --precise 0.2.189
```

`certs/` holds the self-signed test certificate vendored from `s2n-quic-core`, which s2n-tls,
rustls and boring all accept. It is a demo certificate — never use it for anything else. boring
wants filenames rather than PEM bytes, and `baseline/` is a separate crate whose
`CARGO_MANIFEST_DIR` points elsewhere, so `src/quiche_backend.rs` writes the same compiled-in
bytes to a fixed path under the temporary directory and hands boring that.
