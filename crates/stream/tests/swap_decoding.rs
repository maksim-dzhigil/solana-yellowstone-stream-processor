#![allow(clippy::unwrap_used, clippy::expect_used)]

use serde_json::json;
use solana_yellowstone_domain::decoded::{
    DexSwap, extract_token_balance_deltas, infer_swap_from_balance_deltas,
};
use solana_yellowstone_domain::event::{EventIdentity, NormalizedEvent};
use solana_yellowstone_storage::{
    postgres::PostgresEventWriter,
    swaps::{PostgresSwapWriter, SwapWriter},
};

fn token_balance_event(
    slot: u64,
    signature: impl Into<String>,
    program_id: impl Into<String>,
    balances: Vec<(String, String, i64, i64)>,
) -> NormalizedEvent {
    let balances_json: Vec<_> = balances
        .into_iter()
        .map(|(account, mint, pre, post)| {
            json!({
                "account": account,
                "mint": mint,
                "pre": pre,
                "post": post,
            })
        })
        .collect();

    NormalizedEvent::new(
        EventIdentity::Transaction {
            cluster: "localnet".to_owned(),
            slot,
            signature: signature.into(),
            index: 0,
        },
        json!({ "token_balances": balances_json, "program_id": program_id.into() }),
    )
}

#[tokio::test]
#[ignore = "requires local postgres; run `make compose-up test-postgres`"]
async fn transaction_payload_yields_balance_deltas_and_inferred_swap() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must be set for postgres integration tests");

    // Ensure migrations are applied (swaps table must exist).
    let event_writer = PostgresEventWriter::connect(&database_url)
        .await
        .expect("postgres writer should connect and migrate");

    let pool = event_writer.pool().clone();
    let swap_writer = PostgresSwapWriter::from_pool(pool.clone());

    // Synthetic swap: account-a loses 100 of mint-a, account-b gains 100 of mint-b.
    let event = token_balance_event(
        20001,
        "swap-sig-demo",
        "program-raydium",
        vec![
            ("acct-a".to_owned(), "mint-a".to_owned(), 1_000, 900),
            ("acct-b".to_owned(), "mint-b".to_owned(), 500, 600),
        ],
    );

    // Extract token balance deltas from the payload.
    let deltas = extract_token_balance_deltas(&event.payload).expect("should extract deltas");
    assert_eq!(deltas.len(), 2);
    assert_eq!(deltas[0].delta(), 100);
    assert_eq!(deltas[1].delta(), -100);

    // Infer a two-legged swap.
    let program_id = event.payload["program_id"]
        .as_str()
        .expect("program_id in payload");
    let swap = infer_swap_from_balance_deltas(
        event.slot(),
        event.signature().expect("signature present"),
        program_id,
        &deltas,
    )
    .expect("should infer swap");

    let expected = DexSwap {
        slot: 20001,
        signature: "swap-sig-demo".to_owned(),
        program_id: "program-raydium".to_owned(),
        token_in: "mint-a".to_owned(),
        token_in_amount: 100,
        token_out: "mint-b".to_owned(),
        token_out_amount: 100,
    };
    assert_eq!(swap, expected);

    // Write the inferred swap to Postgres.
    let written = swap_writer
        .write_swaps(&[swap])
        .await
        .expect("swap write should succeed");
    assert_eq!(written, 1);

    // Verify the swap row in the database.
    let row: (i64, String, String, i64, String, i64) = sqlx::query_as(
        "SELECT slot, token_in, token_out, token_in_amount, signature, token_out_amount FROM swaps WHERE signature = $1",
    )
    .bind("swap-sig-demo")
    .fetch_one(&pool)
    .await
    .expect("should find swap row");

    assert_eq!(row.0, 20001);
    assert_eq!(row.1, "mint-a");
    assert_eq!(row.2, "mint-b");
    assert_eq!(row.3, 100);
    assert_eq!(row.4, "swap-sig-demo");
    assert_eq!(row.5, 100);
}

#[tokio::test]
#[ignore = "requires local postgres; run `make compose-up test-postgres`"]
async fn ambiguous_payload_does_not_infer_swap() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must be set for postgres integration tests");

    let event_writer = PostgresEventWriter::connect(&database_url)
        .await
        .expect("postgres writer should connect and migrate");

    let pool = event_writer.pool().clone();
    let swap_writer = PostgresSwapWriter::from_pool(pool);

    // Three-legged change: ambiguous, should not infer a clean swap.
    let event = token_balance_event(
        20002,
        "no-swap-sig",
        "program-unknown",
        vec![
            ("acct-a".to_owned(), "mint-a".to_owned(), 1_000, 900),
            ("acct-b".to_owned(), "mint-b".to_owned(), 500, 600),
            ("acct-c".to_owned(), "mint-c".to_owned(), 100, 200),
        ],
    );

    let deltas = extract_token_balance_deltas(&event.payload).expect("should extract deltas");
    let program_id = event.payload["program_id"]
        .as_str()
        .expect("program_id in payload");
    let inferred = infer_swap_from_balance_deltas(
        event.slot(),
        event.signature().unwrap(),
        program_id,
        &deltas,
    );

    assert!(
        inferred.is_err(),
        "three-legged change should not yield a clean swap"
    );

    // Write an empty batch to verify the writer handles empty input.
    let written = swap_writer
        .write_swaps(&[])
        .await
        .expect("empty write should succeed");
    assert_eq!(written, 0);
}
