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
        // Worst iteration-to-iteration spread seen for any mode at a given chunk size.
        let mut spread: BTreeMap<usize, f64> = BTreeMap::new();

        for &chunk in &args.chunks {
            if chunk == 0 {
                bail!("--chunks must be positive");
            }
            let cfg = RunConfig { total_bytes, chunk };

            // All modes run on one connection, round-robin, so warmup and any drift apply to
            // them equally.
            let series = harness
                .run_series(&modes, cfg, args.warmup + args.iters)
                .await?;

            for (index, (&mode, mut timings)) in modes.iter().zip(series).enumerate() {
                timings.drain(..args.warmup);

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

                // How much the same measurement moved between iterations. Deltas smaller than
                // this are noise, not a result.
                let observed = 1.0 - fastest.as_secs_f64() / slowest.as_secs_f64();
                let row = spread.entry(chunk).or_insert(0.0);
                *row = row.max(observed);

                rates.insert((chunk, index), throughput_mib_s(total_bytes, median));
            }
        }

        report(lib, &args, &modes, &rates, &spread);
    }

    Ok(())
}

fn throughput_mib_s(bytes: u64, elapsed: Duration) -> f64 {
    (bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
}

fn report(
    lib: &str,
    args: &Args,
    modes: &[Mode],
    rates: &BTreeMap<(usize, usize), f64>,
    spread: &BTreeMap<usize, f64>,
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
                print!("{:>12}", format!("{} Δ", mode.label()));
            }
        }
    }
    print!("{:>14}", "DATA headers");
    println!("{:>8}", "noise");

    for &chunk in &args.chunks {
        print!("{:>10}", format_bytes(chunk));

        for (index, _) in modes.iter().enumerate() {
            match rates.get(&(chunk, index)) {
                Some(rate) => print!("{:>16}", format!("{rate:.1} MiB/s")),
                None => print!("{:>16}", "-"),
            }
        }

        if let Some(baseline) = baseline {
            let framed = rates.get(&(chunk, baseline)).copied();
            for (index, mode) in modes.iter().enumerate() {
                if *mode == Mode::Framed {
                    continue;
                }
                match (framed, rates.get(&(chunk, index)).copied()) {
                    (Some(framed), Some(rate)) if framed > 0.0 => {
                        print!("{:>12}", format!("{:+.1}%", (rate / framed - 1.0) * 100.0))
                    }
                    _ => print!("{:>12}", "-"),
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

        // Spread between iterations of the same measurement: deltas below it mean nothing.
        match spread.get(&chunk) {
            Some(noise) => println!("{:>8}", format!("±{:.1}%", noise * 100.0)),
            None => println!("{:>8}", "-"),
        }
    }

    println!();
    println!("  DATA headers: what framing costs on the wire, independent of CPU time");
    println!("  noise: spread between iterations of one measurement; smaller deltas mean nothing");
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 && bytes.is_multiple_of(1024) {
        format!("{} KiB", bytes / 1024)
    } else {
        format!("{bytes} B")
    }
}
