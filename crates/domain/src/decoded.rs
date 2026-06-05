use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A single token balance change observed in a transaction or instruction.
///
/// This is a demo-level structure for Solana token balance delta extraction.
/// It is intentionally simple and does not cover all Solana token program
/// edge cases (e.g., wrapped SOL, mint decimals, multi-instruction routing).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenBalanceDelta {
    pub account: String,
    pub mint: String,
    pub pre_amount: i64,
    pub post_amount: i64,
}

impl TokenBalanceDelta {
    /// Delta = pre - post. Positive means the account lost tokens;
    /// negative means the account gained tokens.
    pub fn delta(&self) -> i64 {
        self.pre_amount - self.post_amount
    }
}

/// A simple inferred DEX swap derived from token balance deltas.
///
/// This is **not** a full semantic decoder (Orca, Raydium, Meteora).
/// It is a demo inference that assumes a two-legged swap:
/// one token goes in, one token goes out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DexSwap {
    pub slot: u64,
    pub signature: String,
    pub program_id: String,
    pub token_in: String,
    pub token_in_amount: u64,
    pub token_out: String,
    pub token_out_amount: u64,
}

/// A decoded domain event produced after raw normalization.
///
/// `DecodedEvent` sits on top of `NormalizedEvent` and represents
/// business-layer semantics rather than stream envelope data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecodedEvent {
    TokenBalanceDelta(TokenBalanceDelta),
    DexSwap(DexSwap),
    UnknownProgram(UnknownProgramEvent),
}

/// Placeholder for programs that do not match any known decoder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnknownProgramEvent {
    pub program_id: String,
    pub raw_payload: Value,
}

/// Errors that can occur during decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    InvalidTokenBalance { account: String, reason: String },
    MissingTokenBalances,
    AmbiguousSwap { reason: String },
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTokenBalance { account, reason } => {
                write!(f, "invalid token balance for account {account}: {reason}")
            }
            Self::MissingTokenBalances => {
                write!(f, "payload does not contain token_balances array")
            }
            Self::AmbiguousSwap { reason } => {
                write!(f, "cannot infer a clean two-legged swap: {reason}")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

/// Extract token balance deltas from a `NormalizedEvent` payload.
///
/// Expected payload shape (demo):
/// ```json
/// {
///   "token_balances": [
///     {"account": "...", "mint": "...", "pre": 1000, "post": 900},
///     {"account": "...", "mint": "...", "pre": 500, "post": 600}
///   ]
/// }
/// ```
pub fn extract_token_balance_deltas(payload: &Value) -> Result<Vec<TokenBalanceDelta>, DecodeError> {
    let balances = payload
        .get("token_balances")
        .and_then(Value::as_array)
        .ok_or(DecodeError::MissingTokenBalances)?;

    let mut deltas = Vec::with_capacity(balances.len());
    for entry in balances {
        let account = entry
            .get("account")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let mint = entry
            .get("mint")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let pre_amount = entry
            .get("pre")
            .and_then(Value::as_i64)
            .ok_or_else(|| DecodeError::InvalidTokenBalance {
                account: account.clone(),
                reason: "missing or non-numeric 'pre' field".to_owned(),
            })?;
        let post_amount = entry
            .get("post")
            .and_then(Value::as_i64)
            .ok_or_else(|| DecodeError::InvalidTokenBalance {
                account: account.clone(),
                reason: "missing or non-numeric 'post' field".to_owned(),
            })?;

        deltas.push(TokenBalanceDelta {
            account,
            mint,
            pre_amount,
            post_amount,
        });
    }

    Ok(deltas)
}

/// Infer a simple two-legged swap from token balance deltas.
///
/// Rules (demo-level):
/// 1. Exactly two accounts must have **non-zero** deltas.
/// 2. One account must lose tokens (delta > 0), the other must gain (delta < 0).
/// 3. The mints must be different.
/// 4. The absolute values must match (no fees accounted for).
///
/// If more than two accounts change balance, or if deltas do not form
/// a clean pair, returns `None`.
pub fn infer_swap_from_balance_deltas(
    slot: u64,
    signature: impl Into<String>,
    program_id: impl Into<String>,
    deltas: &[TokenBalanceDelta],
) -> Result<DexSwap, DecodeError> {
    let non_zero: Vec<&TokenBalanceDelta> = deltas.iter().filter(|d| d.delta() != 0).collect();

    if non_zero.len() != 2 {
        return Err(DecodeError::AmbiguousSwap {
            reason: format!("expected exactly 2 non-zero deltas, got {}", non_zero.len()),
        });
    }

    let a = non_zero[0];
    let b = non_zero[1];

    let (sender, receiver) = match (a.delta().signum(), b.delta().signum()) {
        (1, -1) => (a, b),
        (-1, 1) => (b, a),
        _ => {
            return Err(DecodeError::AmbiguousSwap {
                reason: "deltas do not form a clean in/out pair".to_owned(),
            });
        }
    };

    if sender.mint == receiver.mint {
        return Err(DecodeError::AmbiguousSwap {
            reason: "sender and receiver use the same mint".to_owned(),
        });
    }

    let token_in_amount = sender
        .delta()
        .unsigned_abs();
    let token_out_amount = receiver
        .delta()
        .unsigned_abs();

    Ok(DexSwap {
        slot,
        signature: signature.into(),
        program_id: program_id.into(),
        token_in: sender.mint.clone(),
        token_in_amount,
        token_out: receiver.mint.clone(),
        token_out_amount,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        DecodeError, DexSwap, TokenBalanceDelta, extract_token_balance_deltas,
        infer_swap_from_balance_deltas,
    };
    use serde_json::json;

    #[test]
    fn extracts_token_balance_deltas_from_payload() {
        let payload = json!({
            "token_balances": [
                {"account": "acct-a", "mint": "mint-a", "pre": 1000, "post": 900},
                {"account": "acct-b", "mint": "mint-b", "pre": 500, "post": 600},
            ]
        });

        let deltas = extract_token_balance_deltas(&payload).expect("should extract");

        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].delta(), 100);
        assert_eq!(deltas[1].delta(), -100);
    }

    #[test]
    fn infers_simple_two_mint_swap() {
        let deltas = vec![
            TokenBalanceDelta {
                account: "acct-a".to_owned(),
                mint: "mint-a".to_owned(),
                pre_amount: 1000,
                post_amount: 900,
            },
            TokenBalanceDelta {
                account: "acct-b".to_owned(),
                mint: "mint-b".to_owned(),
                pre_amount: 500,
                post_amount: 600,
            },
        ];

        let swap = infer_swap_from_balance_deltas(42, "sig-1", "program-1", &deltas)
            .expect("should infer swap");

        assert_eq!(
            swap,
            DexSwap {
                slot: 42,
                signature: "sig-1".to_owned(),
                program_id: "program-1".to_owned(),
                token_in: "mint-a".to_owned(),
                token_in_amount: 100,
                token_out: "mint-b".to_owned(),
                token_out_amount: 100,
            }
        );
    }

    #[test]
    fn rejects_ambiguous_swap_with_three_deltas() {
        let deltas = vec![
            TokenBalanceDelta {
                account: "a".to_owned(),
                mint: "m-a".to_owned(),
                pre_amount: 100,
                post_amount: 90,
            },
            TokenBalanceDelta {
                account: "b".to_owned(),
                mint: "m-b".to_owned(),
                pre_amount: 50,
                post_amount: 60,
            },
            TokenBalanceDelta {
                account: "c".to_owned(),
                mint: "m-c".to_owned(),
                pre_amount: 10,
                post_amount: 20,
            },
        ];

        let err = infer_swap_from_balance_deltas(1, "sig", "prog", &deltas).expect_err("should fail");
        assert!(matches!(err, DecodeError::AmbiguousSwap { .. }));
    }

    #[test]
    fn rejects_same_mint_as_swap() {
        let deltas = vec![
            TokenBalanceDelta {
                account: "a".to_owned(),
                mint: "same-mint".to_owned(),
                pre_amount: 100,
                post_amount: 90,
            },
            TokenBalanceDelta {
                account: "b".to_owned(),
                mint: "same-mint".to_owned(),
                pre_amount: 50,
                post_amount: 60,
            },
        ];

        let err = infer_swap_from_balance_deltas(1, "sig", "prog", &deltas).expect_err("should fail");
        assert!(matches!(err, DecodeError::AmbiguousSwap { .. }));
    }

    #[test]
    fn rejects_missing_token_balances() {
        let payload = json!({"other_field": 42});

        let err = extract_token_balance_deltas(&payload).expect_err("should fail");
        assert!(matches!(err, DecodeError::MissingTokenBalances));
    }
}
