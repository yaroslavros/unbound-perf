//! Command line, measurement matrix and reporting.
//!
//! Shared verbatim by both binaries: `unbound-perf` builds it against the forked libraries with
//! the `unbound` feature on, and `unbound-perf-baseline` builds this same source against the
//! unmodified upstream libraries with the feature off, where only the framed mode exists.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Result, bail};
use clap::Parser;

use crate::{
    quiche_backend::Quiche,
    quinn_backend::Quinn,
    s2n_backend::S2nQuic,
    tunnel::{Mode, RunConfig},
};

/// Only the framed mode exists without the unbound data API.
#[cfg(feature = "unbound")]
const DEFAULT_MODES: &str = "framed,unbound,handoff";
#[cfg(not(feature = "unbound"))]
const DEFAULT_MODES: &str = "framed";

/// Says which libraries produced the numbers, so two outputs cannot be confused.
#[cfg(feature = "unbound")]
const BUILD: &str = "";
#[cfg(not(feature = "unbound"))]
const BUILD: &str = " (upstream baseline)";

#[derive(Parser, Debug)]
#[command(
    about = "Compares CONNECT tunnel throughput with and without unbound data (draft-rosomakho-httpbis-h3-unbound-data)"
)]
struct Args {
    /// QUIC implementations to measure
    #[arg(long, value_delimiter = ',', default_value = "quinn,s2n-quic,quiche")]
    libs: Vec<String>,

    /// Tunnel modes to measure: framed, unbound, handoff
    #[arg(long, value_delimiter = ',', default_value = DEFAULT_MODES)]
    modes: Vec<String>,

    /// Payload chunk sizes in bytes; the tunnel does one write per chunk
    #[arg(long, value_delimiter = ',', default_value = "64,512,4096,16384,65536")]
    chunks: Vec<usize>,

    /// Payload pushed through the tunnel per run, in MiB
    ///
    /// Shorter runs are noticeably noisier; 128 MiB keeps the spread at a few percent.
    #[arg(long, default_value_t = 128)]
    total_mib: u64,

    /// Timed runs per combination; the fastest one is reported
    #[arg(long, default_value_t = 5)]
    iters: usize,

    /// Untimed rounds before the timed ones
    ///
    /// Throughput ramps over the first couple of transfers on a fresh connection, so a couple of
    /// untimed rounds matter more than extra timed ones.
    #[arg(long, default_value_t = 3)]
    warmup: usize,

    /// Print every timed iteration, not just the best one
    #[arg(long)]
    show_runs: bool,
}

enum Harness {
    Quinn(Quinn),
    S2nQuic(S2nQuic),
    Quiche(Quiche),
}

impl Harness {
    fn new(lib: &str) -> Result<Self> {
        match lib {
            "quinn" => Ok(Self::Quinn(Quinn::new()?)),
            "s2n-quic" | "s2n" => Ok(Self::S2nQuic(S2nQuic::new()?)),
            "quiche" => Ok(Self::Quiche(Quiche::new()?)),
            other => {
                bail!("unknown implementation `{other}`, expected quinn, s2n-quic or quiche")
            }
        }
    }

    async fn run_series(
        &mut self,
        modes: &[Mode],
        cfg: RunConfig,
        rounds: usize,
    ) -> Result<Vec<Vec<Duration>>> {
        match self {
            Self::Quinn(harness) => harness.run_series(modes, cfg, rounds).await,
            Self::S2nQuic(harness) => harness.run_series(modes, cfg, rounds).await,
            Self::Quiche(harness) => harness.run_series(modes, cfg, rounds).await,
        }
    }
}

pub async fn run() -> Result<()> {
    let args = Args::parse();

    if args.iters == 0 {
        bail!("--iters must be at least 1");
    }

    let modes = args
        .modes
        .iter()
        .map(|mode| Mode::parse(mode))
        .collect::<Result<Vec<_>>>()?;
    let total_bytes = args.total_mib * 1024 * 1024;

    for lib in &args.libs {
        let mut harness = Harness::new(lib)?;
        let mut rates: BTreeMap<(usize, usize), f64> = BTreeMap::new();
        let mut deltas: BTreeMap<(usize, usize), Paired> = BTreeMap::new();
        // Worst iteration-to-iteration spread seen for any mode at a given chunk size.
        let mut drift: BTreeMap<usize, f64> = BTreeMap::new();
        // `handoff` paired against `unbound` rather than against framed. Framing is already gone
        // from both, so this is the pair that isolates what the HTTP/3 layer itself costs — and
        // it needs to be paired for the same reason every other comparison here does.
        let mut layer: BTreeMap<usize, Paired> = BTreeMap::new();
        let framed = modes.iter().position(|mode| *mode == Mode::Framed);
        let against_unbound = handoff_against_unbound(&modes);

        for &chunk in &args.chunks {
            if chunk == 0 {
                bail!("--chunks must be positive");
            }
            let cfg = RunConfig { total_bytes, chunk };

            // All modes run on one connection, round-robin, so warmup and any drift apply to
            // them equally.
            let mut series = harness
                .run_series(&modes, cfg, args.warmup + args.iters)
                .await?;

            for timings in &mut series {
                timings.drain(..args.warmup);
            }

            for (index, (&mode, timings)) in modes.iter().zip(&series).enumerate() {
                let fastest = timings.iter().copied().min().expect("at least one run");
                let slowest = timings.iter().copied().max().expect("at least one run");

                // The median, not the fastest run: with this much jitter, "best of N" reports
                // whichever cell got the luckiest iteration, which invents deltas that are not
                // there.
                let median = {
                    let mut sorted = timings.clone();
                    sorted.sort();
                    sorted[sorted.len() / 2]
                };

                if args.show_runs {
                    println!(
                        "  {lib} {:>7} {:<8} median {:>6.1} MiB/s  runs: {}",
                        format_bytes(chunk),
                        mode.label(),
                        throughput_mib_s(total_bytes, median),
                        timings
                            .iter()
                            .map(|elapsed| format!(
                                "{:.1}",
                                throughput_mib_s(total_bytes, *elapsed)
                            ))
                            .collect::<Vec<_>>()
                            .join(" ")
                    );
                }

                // Kept only as a health signal for the machine. It is *not* the uncertainty of
                // the delta below, which is measured within rounds and so cancels most of it.
                let observed = 1.0 - fastest.as_secs_f64() / slowest.as_secs_f64();
                let row = drift.entry(chunk).or_insert(0.0);
                *row = row.max(observed);

                rates.insert((chunk, index), throughput_mib_s(total_bytes, median));

                if let Some(framed) = framed.filter(|framed| *framed != index) {
                    deltas.insert((chunk, index), Paired::new(&series[framed], timings));
                }
            }

            if let Some((unbound, handoff)) = against_unbound {
                layer.insert(chunk, Paired::new(&series[unbound], &series[handoff]));
            }
        }

        report(lib, &args, &modes, &rates, &deltas, &drift, &layer);
    }

    Ok(())
}

/// The indices of `unbound` and `handoff`, when the run covers both.
///
/// Only those two are worth pairing against each other, so this is deliberately not a general
/// "every mode against every other" matrix.
#[cfg(feature = "unbound")]
fn handoff_against_unbound(modes: &[Mode]) -> Option<(usize, usize)> {
    let find = |target: Mode| modes.iter().position(|mode| *mode == target);

    Some((find(Mode::Unbound)?, find(Mode::Handoff)?))
}

/// The upstream baseline build has only the framed mode, so there is never a pair here.
#[cfg(not(feature = "unbound"))]
fn handoff_against_unbound(_modes: &[Mode]) -> Option<(usize, usize)> {
    None
}

/// How much faster a mode ran than a reference mode, measured round by round.
///
/// The modes are interleaved on one connection, so round `k` of every mode ran in the same time
/// window on the same machine. Differencing *within* a round therefore cancels the drift that
/// dominates each mode's own spread — which is the whole reason the per-mode spread is a poor
/// error bar for a comparison between modes, however large it looks.
struct Paired {
    /// Median of the per-round deltas.
    median: f64,
    /// Half the interquartile range of the per-round deltas.
    spread: f64,
    /// How many rounds came out in this mode's favour.
    agree: usize,
    rounds: usize,
}

impl Paired {
    fn new(reference: &[Duration], mode: &[Duration]) -> Self {
        let mut deltas: Vec<f64> = reference
            .iter()
            .zip(mode)
            .map(|(reference, mode)| reference.as_secs_f64() / mode.as_secs_f64() - 1.0)
            .collect();

        let rounds = deltas.len();
        let agree = deltas.iter().filter(|delta| **delta > 0.0).count();

        deltas.sort_by(f64::total_cmp);

        // Interquartile, not full range: pairing cancels drift that is slow next to a round, but
        // a round that happens to collide with a burst of unrelated work produces one wild
        // difference, and a full range would let that single round speak for the whole series.
        let quartile =
            |fraction: f64| deltas[((rounds as f64 * fraction) as usize).min(rounds - 1)];

        Self {
            median: deltas[rounds / 2],
            spread: (quartile(0.75) - quartile(0.25)) / 2.0,
            agree,
            rounds,
        }
    }
}

impl std::fmt::Display for Paired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:+.1}% ±{:.1} {}/{}",
            self.median * 100.0,
            self.spread * 100.0,
            self.agree,
            self.rounds
        )
    }
}

fn throughput_mib_s(bytes: u64, elapsed: Duration) -> f64 {
    (bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
}

fn report(
    lib: &str,
    args: &Args,
    modes: &[Mode],
    rates: &BTreeMap<(usize, usize), f64>,
    deltas: &BTreeMap<(usize, usize), Paired>,
    drift: &BTreeMap<usize, f64>,
    layer: &BTreeMap<usize, Paired>,
) {
    println!();
    println!(
        "{lib}{BUILD} — {} MiB per run, median of {} ({} warmup)",
        args.total_mib, args.iters, args.warmup
    );
    println!();

    print!("{:>10}", "chunk");
    for mode in modes {
        print!("{:>16}", mode.label());
    }
    let baseline = modes.iter().position(|mode| *mode == Mode::Framed);
    if baseline.is_some() {
        for mode in modes {
            if *mode != Mode::Framed {
                print!("{:>21}", format!("{} Δ", mode.label()));
            }
        }
    }
    print!("{:>14}", "DATA headers");
    println!("{:>8}", "drift");

    for &chunk in &args.chunks {
        print!("{:>10}", format_bytes(chunk));

        for (index, _) in modes.iter().enumerate() {
            match rates.get(&(chunk, index)) {
                Some(rate) => print!("{:>16}", format!("{rate:.1} MiB/s")),
                None => print!("{:>16}", "-"),
            }
        }

        if baseline.is_some() {
            for (index, mode) in modes.iter().enumerate() {
                if *mode == Mode::Framed {
                    continue;
                }
                // Padded as a string: the `Display` impl writes through `write!`, which does
                // not itself honour a width.
                match deltas.get(&(chunk, index)) {
                    Some(paired) => print!("{:>21}", paired.to_string()),
                    None => print!("{:>21}", "-"),
                }
            }
        }

        // What the framing costs on the wire, independent of CPU time.
        let cfg = RunConfig {
            total_bytes: args.total_mib * 1024 * 1024,
            chunk,
        };
        let share = cfg.framing_overhead() as f64 / cfg.total_bytes as f64 * 100.0;
        print!("{:>14}", format!("{share:.2}%"));

        // How much the machine itself moved during the run.
        match drift.get(&chunk) {
            Some(drift) => println!("{:>8}", format!("±{:.1}%", drift * 100.0)),
            None => println!("{:>8}", "-"),
        }
    }

    // `handoff` against `unbound`, which the main table cannot show: both of its delta columns
    // are measured against framed, and dividing one by the other would throw the pairing away
    // again. Framing is gone from both of these, so what is left is the HTTP/3 layer itself.
    if !layer.is_empty() {
        println!();
        println!("  handoff against unbound — what the HTTP/3 layer costs once framing is gone:");
        println!();

        for &chunk in &args.chunks {
            match layer.get(&chunk) {
                Some(paired) => {
                    println!("{:>10}{:>21}", format_bytes(chunk), paired.to_string())
                }
                None => println!("{:>10}{:>21}", format_bytes(chunk), "-"),
            }
        }
    }

    println!();

    // The upstream baseline build has only the framed mode, so there is nothing to compare and
    // no delta column to explain.
    if !deltas.is_empty() {
        println!(
            "  Δ: median of the per-round differences against framed, ± half their interquartile"
        );
        println!("     range, then how many rounds came out in the mode's favour. The modes are");
        println!("     interleaved, so each round compares them in the same time window.");
    }

    println!("  DATA headers: what framing costs on the wire, independent of CPU time");
    println!("  drift: how much one mode's own throughput moved between rounds. A health signal");

    if deltas.is_empty() {
        println!("     for the machine; it is not an error bar for a comparison.");
    } else {
        println!("     for the machine, not an error bar for Δ — the paired comparison cancels");
        println!("     most of it, so a drifting machine can still give a tight Δ.");
    }
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 && bytes.is_multiple_of(1024) {
        format!("{} KiB", bytes / 1024)
    } else {
        format!("{bytes} B")
    }
}
