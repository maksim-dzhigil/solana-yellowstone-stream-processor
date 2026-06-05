use async_trait::async_trait;
use solana_yellowstone_domain::decoded::DexSwap;
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::fmt;

#[async_trait]
pub trait SwapWriter {
    type Error;

    async fn write_swaps(&self, swaps: &[DexSwap]) -> Result<usize, Self::Error>;
}

#[derive(Debug, Clone)]
pub struct PostgresSwapWriter {
    pool: PgPool,
}

impl PostgresSwapWriter {
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SwapWriter for PostgresSwapWriter {
    type Error = PostgresSwapWriteError;

    async fn write_swaps(&self, swaps: &[DexSwap]) -> Result<usize, Self::Error> {
        if swaps.is_empty() {
            return Ok(0);
        }

        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO swaps (slot, signature, program_id, token_in, token_in_amount, token_out, token_out_amount) ",
        );

        query.push_values(swaps.iter(), |mut builder, swap| {
            builder
                .push_bind(i64::try_from(swap.slot).unwrap_or(-1))
                .push_bind(&swap.signature)
                .push_bind(&swap.program_id)
                .push_bind(&swap.token_in)
                .push_bind(i64::try_from(swap.token_in_amount).unwrap_or(-1))
                .push_bind(&swap.token_out)
                .push_bind(i64::try_from(swap.token_out_amount).unwrap_or(-1));
        });

        let result = query
            .build()
            .execute(&self.pool)
            .await
            .map_err(PostgresSwapWriteError::Sqlx)?;

        Ok(usize::try_from(result.rows_affected()).unwrap_or(usize::MAX))
    }
}

#[derive(Debug)]
pub enum PostgresSwapWriteError {
    Sqlx(sqlx::Error),
}

impl fmt::Display for PostgresSwapWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlx(err) => write!(f, "postgres swap write failed: {err}"),
        }
    }
}

impl std::error::Error for PostgresSwapWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlx(err) => Some(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PostgresSwapWriter, SwapWriter};
    use solana_yellowstone_domain::decoded::DexSwap;

    #[tokio::test]
    #[ignore = "requires local postgres; run `make compose-up test-postgres`"]
    async fn writes_swaps_to_postgres() {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set for postgres integration tests");

        let pool = sqlx::PgPool::connect(&database_url)
            .await
            .expect("should connect to postgres");

        let writer = PostgresSwapWriter::from_pool(pool.clone());

        let swap = DexSwap {
            slot: 10_001,
            signature: "swap-sig-1".to_owned(),
            program_id: "program-1".to_owned(),
            token_in: "mint-a".to_owned(),
            token_in_amount: 1_000,
            token_out: "mint-b".to_owned(),
            token_out_amount: 2_500,
        };

        let written = writer
            .write_swaps(std::slice::from_ref(&swap))
            .await
            .expect("write should succeed");

        assert_eq!(written, 1);

        let row: (i64, String, i64) =
            sqlx::query_as("SELECT slot, token_in, token_in_amount FROM swaps WHERE signature = $1")
                .bind(&swap.signature)
                .fetch_one(&pool)
                .await
                .expect("select should return row");

        assert_eq!(row.0, 10_001);
        assert_eq!(row.1, "mint-a");
        assert_eq!(row.2, 1_000);
    }
}
