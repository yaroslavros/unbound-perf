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
- **`handoff against unbound`**, the block under the table, is the same paired statistic with
  `unbound` as the reference instead of framed. Framing is gone from both of those, so what is
  left is what the HTTP/3 layer itself costs. It gets its own block rather than a column because
  dividing the two `Δ` columns would discard the pairing and put us back where we started.
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
  (427 → 918 MiB/s at 64 KiB) and compressed every delta along with it. Worth re-checking after
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

     chunk          framed         unbound         handoff            unbound Δ            handoff Δ  DATA headers   drift
      64 B       9.0 MiB/s      34.4 MiB/s      33.9 MiB/s  +289.2% ±23.7 10/10  +276.5% ±24.3 10/10         4.69%  ±16.2%
     512 B     220.1 MiB/s     535.3 MiB/s     571.9 MiB/s  +156.1% ±23.0 10/10  +160.4% ±21.7 10/10         0.59%  ±15.2%
     4 KiB     882.0 MiB/s     990.3 MiB/s    1108.9 MiB/s    +12.2% ±0.5 10/10    +25.6% ±0.5 10/10         0.07%   ±6.7%
    16 KiB    1015.6 MiB/s    1044.5 MiB/s    1158.4 MiB/s     +2.9% ±0.1 10/10    +14.2% ±0.2 10/10         0.03%   ±2.5%
    64 KiB    1049.8 MiB/s    1057.5 MiB/s    1176.8 MiB/s     +1.0% ±0.2 10/10    +12.1% ±0.3 10/10         0.01%   ±4.3%

s2n-quic — 128 MiB per run, median of 10 (3 warmup)

     chunk          framed         unbound         handoff            unbound Δ            handoff Δ  DATA headers   drift
      64 B     100.4 MiB/s     253.3 MiB/s     309.8 MiB/s   +152.5% ±0.4 10/10   +208.8% ±0.4 10/10         4.69%   ±4.1%
     512 B     536.7 MiB/s     806.6 MiB/s     926.7 MiB/s    +50.3% ±0.6 10/10    +72.7% ±0.8 10/10         0.59%  ±11.9%
     4 KiB    1039.7 MiB/s    1134.4 MiB/s    1194.4 MiB/s     +9.2% ±0.3 10/10     +14.9% ±1.1 9/10         0.07%  ±20.3%
    16 KiB    1177.2 MiB/s    1211.1 MiB/s    1254.8 MiB/s     +2.9% ±0.5 10/10     +6.7% ±0.1 10/10         0.03%   ±3.4%
    64 KiB    1225.8 MiB/s    1236.9 MiB/s    1275.3 MiB/s     +0.8% ±0.1 10/10     +3.9% ±0.2 10/10         0.01%   ±2.7%

quiche — 128 MiB per run, median of 10 (3 warmup)

     chunk          framed         unbound         handoff            unbound Δ            handoff Δ  DATA headers   drift
      64 B      65.1 MiB/s      82.3 MiB/s      87.9 MiB/s    +27.3% ±1.0 10/10    +36.0% ±1.3 10/10         4.69%   ±6.6%
     512 B     366.5 MiB/s     441.2 MiB/s     460.8 MiB/s    +17.8% ±1.6 10/10    +23.1% ±1.7 10/10         0.59%   ±6.0%
     4 KiB     839.4 MiB/s     878.8 MiB/s     887.8 MiB/s     +4.4% ±0.4 10/10     +5.8% ±0.5 10/10         0.07%   ±1.8%
    16 KiB     886.4 MiB/s     903.3 MiB/s     906.9 MiB/s     +1.8% ±0.2 10/10     +2.3% ±0.3 10/10         0.03%  ±17.1%
    64 KiB     917.9 MiB/s     924.1 MiB/s     924.8 MiB/s      +0.7% ±0.3 9/10      +0.7% ±0.6 7/10         0.01%   ±7.6%
```

All three tell the same story, and the two columns of deltas answer two different questions.

Read the agreement counts first. Of the thirty delta cells above, twenty-eight are unanimous and
one more is 9/10 — these are not marginal effects. The single exception is quiche's `handoff` at
64 KiB, at 7/10, and that one matters; see finding 2.

**1. Dropping the framing is what wins on small writes.** At 64 B the gain is +289% (quinn),
+153% (s2n-quic) and +27% (quiche), every one of them unanimous across all ten rounds, and in
every case far more than the 4.69% of header bytes can explain. A `DATA` frame is written as
*two* pieces: the frame header and the payload go to the transport separately, so a framed chunk
costs two writes into the QUIC stream where an unbound chunk costs one. (quiche is no different
here: `send_body_zc` tries to fold the header into the payload buffer with `try_add_prefix`, but
`BufSplit for Bytes` does not override it, so the default `false` applies and the header gets its
own `stream_send`.) At 64 B that per-write cost dominates everything else. It then amortises away
as chunks grow: by 64 KiB, `unbound` is only +1.0% / +0.8% / +0.7% over `framed` — still
unanimous or near enough in all three, but no longer a reason to choose anything.

How much *percentage* that is worth depends entirely on how expensive the framed path was to
begin with, which is why the three columns are not comparable. quiche moves 65.1 MiB/s at 64 B
framed where quinn manages 9.0 — more than 7× — so quiche simply has far less to recover. The
likely reason is visible in finding 2: h3's borrowed-slice write makes quinn-proto allocate and
copy on *every* write, and a framed 64 B chunk costs two of them.

**2. Handing the stream back keeps winning at every chunk size, on h3** — +12.1% (quinn) and
+3.9% (s2n-quic) over framed at 64 KiB, at ±0.3 and ±0.2 with every round agreeing. That is not
about framing, which is already gone in both modes being compared. It is a per-byte cost in h3's
send path: h3 passes the
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

At 64 KiB, with `handoff` compared against `unbound` rather than against framed — the pair that
isolates this, and what the tool's second block reports:

| 64 KiB, handoff over unbound | gain |
| --- | --- |
| quinn | +11.3% |
| s2n-quic | +3.1% |
| quiche | +0.1% |

quiche's is nothing, and the agreement count is what settles it: quiche's `handoff` at 64 KiB is
the only cell in the whole matrix that fails to reach 9/10. At **7/10** it is a coin flip, in a
table where twenty-eight of thirty cells are unanimous. The same mode is unanimous at every
smaller chunk size, and quinn's and s2n-quic's handoff columns are unanimous at *every* chunk
size including 64 KiB. So this is not a measurement that was too weak to see an effect; it is the
one place the effect is absent.

That is exactly what the prediction was — an implementation already zero-copy on both sides of
the switch has no copy left for the handoff to remove — so the large-chunk handoff gain on h3 is
a property of h3's send API, not of unbound data. It also means the h3 figures would shrink if
[hyperium/h3#78](https://github.com/hyperium/h3/issues/78) were fixed. **For estimating what the
draft itself buys at large chunk sizes, `unbound` is the honest column, not `handoff`.**

**3. Taking the stream back still wins on small writes, for a different reason.** quiche's
handoff beats its own unbound mode by +6.8% at 64 B, +4.4% at 512 B, +1.0% at 4 KiB, +0.4% at
16 KiB and +0.1% at 64 KiB — a clean monotonic decay to nothing, which is the signature of a
per-call cost rather than a per-byte one. Nothing is being copied there, so this is not finding 2
in miniature. The likely cause is the per-*write* bookkeeping of `quiche::h3::Connection` — the
stream lookup, the state checks, the capacity arithmetic in `do_send_body` — which a released
stream skips by going straight to `stream_send_zc`. Like the framing itself, it is a per-call
cost that disappears into the per-byte cost as chunks grow. This is a reading of the code, not
something the harness isolates; confirming it would need a profile.

**4. The pairing is what makes the small deltas readable, and these tables prove it.** Look at
quiche at 16 KiB: the machine's own throughput wandered by ±17.1% across the run, yet the paired
deltas are +1.8% ±0.2 and +2.3% ±0.3 with all ten rounds agreeing. An unpaired reading of that
row would have thrown away a repeatable 2% because something unrelated moved the machine between
rounds — and an earlier version of this tool did exactly that, printing the ±17.1% as `noise` and
advising you to disregard anything smaller. s2n-quic at 4 KiB is the same story at ±20.3%.

The pairing is not a cure-all. quinn at 64 B and 512 B still carries ±22-24 on its *paired*
deltas, because at 9 MiB/s a 128 MiB round takes long enough to straddle whatever else the
machine is doing. Those rows are fine for "+289%, unanimous" and useless for ordering `unbound`
against `handoff` — which is why quinn's 64 B handoff reads 1.5% *below* its unbound mode here
and above it in an earlier run. Neither ordering means anything.

### The platform matters more than you would think

An earlier run of the same code on an Apple M4 Max (macOS 27.0) capped out near 240-310 MiB/s in
*every* mode, and the handoff gain at 64 KiB shrank to +1.9% (quinn) and +0.6% (s2n-quic) against
the +12.1% and +3.9% above — which reads as "no difference at large chunks". macOS gives none of
these libraries the UDP batching and segmentation offload they use on Linux, so the transport
path there is expensive enough to hide the HTTP/3 layer's per-byte cost entirely: the bottleneck
was never HTTP/3.

The GSO bug above is the same lesson from the other direction: one missing sockopt on one socket
halved quiche's framed throughput on this very machine (427 → 918 MiB/s at 64 KiB), not because
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
918 MiB/s against quinn's 1050 (−13%) and s2n-quic's 1226 (−25%). Some of that is simply datagram
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
