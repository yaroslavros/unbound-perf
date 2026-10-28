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

- **`unbound Δ` / `handoff Δ`** is the result. It is a *paired* comparison: because the modes are
  interleaved round-robin, round `k` of every mode ran in the same time window on the same
  machine, so the delta is computed round by round and then summarised. Each cell reads
  `+3.4% ±1.4 10/10` — the median per-round difference against framed, half the interquartile
  range of those differences, and how many of the rounds came out in the mode's favour. The last
  of those is the one to read first: `10/10` is a consistent difference, `5/10` is a coin flip
  whatever the median says.
- **`DATA headers`** is what framing costs on the wire for that chunk size — purely arithmetic,
  no measurement involved. It is tiny except for very small chunks.
- **`drift`** is how much a single mode's own throughput moved between rounds. It is a health
  signal for the machine, **not** an error bar for `Δ`. Pairing cancels most of what it measures,
  so a machine drifting ±9% can still produce a `Δ` of ±1.4 with every round agreeing. Treat a
  large `drift` as a reason to distrust the *absolute* MiB/s, and read `Δ`'s own spread and
  agreement count for the comparison.

An earlier version of this tool reported only the marginal spread, under the name `noise`, and
told you to disregard any delta smaller than it. That advice was wrong: it discards real,
repeatable differences because an unrelated process moved the whole machine between rounds. The
paired columns replace it.

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
  that drift to whichever ran last. It is also what makes the comparison *paired* — the three
  modes in round `k` are three measurements of the same machine at the same moment, so their
  differences carry far less uncertainty than any one of them does on its own. The reporting
  differences them within each round rather than comparing two independently-summarised columns,
  which is where almost all of the statistical power in this tool comes from.
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
  and the run measures syscall rate instead of protocol work — it cost a factor of two on Linux
  (427 → 924 MiB/s at 64 KiB) and compressed every delta along with it. Worth re-checking after
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

quiche — 128 MiB per run, median of 10 (3 warmup), pinned to a performance core

     chunk          framed         unbound         handoff   unbound Δ   handoff Δ  DATA headers   noise
      64 B      68.0 MiB/s      87.4 MiB/s      93.8 MiB/s      +28.5%      +37.8%         4.69%   ±3.8%
     512 B     376.2 MiB/s     445.2 MiB/s     467.2 MiB/s      +18.3%      +24.2%         0.59%   ±1.9%
     4 KiB     847.2 MiB/s     886.4 MiB/s     897.3 MiB/s       +4.6%       +5.9%         0.07%   ±4.6%
    16 KiB     889.0 MiB/s     905.7 MiB/s     911.9 MiB/s       +1.9%       +2.6%         0.03%  ±20.4%
    64 KiB     924.4 MiB/s     932.0 MiB/s     935.1 MiB/s       +0.8%       +1.2%         0.01%  ±14.9%
```

> These three tables predate the paired `Δ` reporting described above, so their `Δ` is a ratio of
> medians and their last column is the old marginal `noise`. Re-running `--iters 10` on the
> current build adds the per-round spread and the rounds-agreeing count, which is what actually
> settles the small deltas in the 4 KiB-and-up rows — quiche's ±20.4% at 16 KiB says nothing
> about whether its +2.6% is real, and the old column invited exactly that misreading.

All three tell the same story, and the two columns of deltas answer two different questions.

**1. Dropping the framing is what wins on small writes.** At 64 B the gain is 257% (quinn), 152%
(s2n-quic) and 29% (quiche) — in every case far more than the 4.69% of header bytes can explain.
A `DATA` frame is written as *two* pieces: the frame header and the payload go to the transport
separately, so a framed chunk costs two writes into the QUIC stream where an unbound chunk costs
one. (quiche is no different here: `send_body_zc` tries to fold the header into the payload
buffer with `try_add_prefix`, but `BufSplit for Bytes` does not override it, so the default
`false` applies and the header gets its own `stream_send`.) At 64 B that per-write cost dominates
everything else. It then amortises away as chunks grow: by 64 KiB, `unbound` is only +0.8% /
+1.1% / +0.8% over `framed`.

How much *percentage* that is worth depends entirely on how expensive the framed path was to
begin with, which is why the three columns are not comparable. quiche moves 68.0 MiB/s at 64 B
framed where quinn manages 8.6 — nearly 8× — so quiche simply has far less to recover. The likely
reason is visible in finding 2: h3's borrowed-slice write makes quinn-proto allocate and copy on
*every* write, and a framed 64 B chunk costs two of them.

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

**quiche is the control for that claim, and it confirms it.** tokio-quiche is already zero-copy
on both sides of the switch — `send_body_zc` and `stream_send_zc` both take the buffer rather
than a borrowed slice — so there is no copy for its handoff to remove. Comparing `handoff`
against `unbound` directly, which is the pair that isolates it:

| 64 KiB, handoff over unbound | gain |
| --- | --- |
| quinn | +12.8% |
| s2n-quic | +3.2% |
| quiche | +0.3% |

quiche's gain rounds to nothing, and stays there from 4 KiB up (+1.2%, +0.7%, +0.3% at
4/16/64 KiB). These were measured under the old unpaired reporting, so there is no per-round
spread behind them and the honest claim is the comparative one rather than "quiche's is exactly
zero": quinn's +12.8% is an order of magnitude larger, so whatever the handoff saves on h3 at
large chunks, quiche is not paying it. That is what the prediction was — an implementation
already zero-copy on both sides of the switch has no copy left for the handoff to remove — and it
also means the h3 figures would shrink if
[hyperium/h3#78](https://github.com/hyperium/h3/issues/78) were fixed. For estimating what the
draft itself buys at large chunk sizes, `unbound` is the honest column, not `handoff`.

**3. Taking the stream back still wins on small writes, for a different reason.** quiche's
handoff beats its own unbound mode by +4.9% at 512 B and +7.3% at 64 B — the two rows where it is
clearly ahead, and the only place in the quiche table where the handoff buys anything at all.
Nothing is being copied there, so this is not finding 2 in miniature. The likely cause
is the per-*write* bookkeeping of `quiche::h3::Connection` — the stream lookup, the state checks,
the capacity arithmetic in `do_send_body` — which a released stream skips by going straight to
`stream_send_zc`. Like the framing itself, it is a per-call cost that disappears into the
per-byte cost as chunks grow. This is a reading of the code, not something the harness isolates;
confirming it would need a profile.

**4. The rows that matter most are the ones a noisy machine ruins.** The large deltas sit at 64 B
and 512 B, where the machine also wanders most, and they survive it easily — nothing moves a
+257% result. The rows where a few percent *is* the whole answer, 4 KiB and up, are the fragile
ones: quiche's ±14.9% and ±20.4% marginal spread at 64 and 16 KiB is many times its own +1.2% and
+2.6% deltas. Those deltas are not therefore noise — they are the same sign in every table at
every chunk size, which is not what noise looks like — but the old marginal column could not tell
you that, which is precisely why the paired `Δ` replaced it. Pinning the run to one performance
core, as the quiche table above does, helps the absolute numbers; the paired comparison is what
makes the small deltas readable.

### The platform matters more than you would think

An earlier run of the same code on an Apple M4 Max (macOS 27.0) capped out near 240-310 MiB/s in
*every* mode, and the handoff gain at 64 KiB shrank to +1.9% (quinn) and +0.6% (s2n-quic) — which
reads as "no difference at large chunks". macOS gives none of these libraries the UDP batching
and segmentation offload they use on Linux, so the transport path there is expensive enough to
hide the HTTP/3 layer's per-byte cost entirely: the bottleneck was never HTTP/3.

The GSO bug above is the same lesson from the other direction: one missing sockopt on one socket
halved quiche's framed throughput on this very machine (427 → 924 MiB/s at 64 KiB), not because
anything about HTTP/3 changed, but because the syscall rate had become the limit. Any time this
harness reports "no difference", the first question is whether the transport was fast enough for
a difference to be visible at all.

That is worth keeping in mind when reading any result from this harness. It measures the protocol
work per byte against whatever the local transport path costs; make the transport cheap, as Linux
does, and h3's share of the work becomes visible. The Linux numbers above are the ones to use for
a server deployment.

Comparing the tables against each other is not the point — the implementations have different
defaults and the harness only equalises flow-control windows and UDP offloads. Each table's delta
columns are the result.

That is worth insisting on, because the ceilings do differ: at 64 KiB framed, quiche reaches
924 MiB/s against quinn's 980 (−6%) and s2n-quic's 1156 (−20%). Some of that is simply datagram
size — quiche sends 1350-byte datagrams where quinn sends 1452, which is ~7% of the packet rate
on its own — and the rest is transport and crypto defaults this harness does not try to
normalise. None of it is about HTTP/3, and none of it moves the delta columns, which is what the
tool exists to measure. A lower ceiling here is a hint to go looking at socket options, not a
verdict on a library: the first quiche run capped at ~425 MiB/s purely because its client socket
had no GSO, and fixing that one line doubled it.

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
