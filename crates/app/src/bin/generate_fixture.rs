use clap::Parser;
use serde_json::json;
use solana_yellowstone_domain::event::{EventIdentity, NormalizedEvent};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "generate-fixture")]
#[command(about = "Generate synthetic JSONL replay fixtures for benchmarking")]
struct Args {
    #[arg(short, long, default_value_t = 1_000_000)]
    count: usize,

    #[arg(short, long, default_value = "fixtures/synthetic.jsonl")]
    output: PathBuf,

    #[arg(long, default_value_t = 0.0)]
    duplicate_ratio: f64,

    #[arg(long, default_value_t = 100)]
    events_per_slot: usize,
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    if args.duplicate_ratio < 0.0 || args.duplicate_ratio > 1.0 {
        eprintln!("duplicate-ratio must be between 0.0 and 1.0");
        std::process::exit(1);
    }

    let file = File::create(&args.output)?;
    let mut writer = BufWriter::new(file);

    let program_ids = [
        "11111111111111111111111111111111",
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
    ];

    let mut events: Vec<NormalizedEvent> = Vec::with_capacity(args.count);

    for i in 0..args.count {
        let slot = (i / args.events_per_slot + 1) as u64;
        let signature = format!("sig-{slot:016x}-{i:08x}");
        let index = (i % args.events_per_slot) as u64;

        let identity = match i % 4 {
            0 => EventIdentity::Transaction {
                cluster: "mainnet-beta".to_owned(),
                slot,
                signature: signature.clone(),
                index,
            },
            1 => EventIdentity::Instruction {
                cluster: "mainnet-beta".to_owned(),
                slot,
                signature: signature.clone(),
                transaction_index: index,
                instruction_index: (index % 16) as u16,
                inner_instruction_index: None,
                program_id: program_ids[i % program_ids.len()].to_owned(),
            },
            2 => EventIdentity::Account {
                cluster: "mainnet-beta".to_owned(),
                slot,
                account: format!("acct-{slot:016x}-{index:04x}"),
                write_version: index,
                txn_signature: Some(signature.clone()),
                is_startup: false,
            },
            _ if index as usize == args.events_per_slot - 1 => EventIdentity::Slot {
                cluster: "mainnet-beta".to_owned(),
                slot,
                status: solana_yellowstone_domain::event::SlotStatus::Finalized,
            },
            _ => EventIdentity::Transaction {
                cluster: "mainnet-beta".to_owned(),
                slot,
                signature: signature.clone(),
                index,
            },
        };

        let event = NormalizedEvent::new(identity, json!({"index": i}));
        events.push(event);
    }

    if args.duplicate_ratio > 0.0 {
        let duplicate_count = (args.count as f64 * args.duplicate_ratio) as usize;
        for _ in 0..duplicate_count {
            let target = fastrand::usize(0..args.count);
            let source = fastrand::usize(0..args.count);
            events[target] = events[source].clone();
        }
    }

    for event in events {
        let line = serde_json::to_string(&event).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e)
        })?;
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
    }

    writer.flush()?;

    println!(
        "Generated {} events (duplicate_ratio={}) -> {}",
        args.count,
        args.duplicate_ratio,
        args.output.display()
    );

    Ok(())
}
