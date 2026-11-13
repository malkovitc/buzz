use super::*;

#[tokio::test]
async fn receipt_pruning_bounds_bytes_per_credential_and_globally() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state).await.unwrap();
    let bearer = fixture.issue(&store).await;
    let credential = crate::credential::CredentialDigest::from_bearer(&bearer).unwrap();
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .unwrap();
    for request_id in ["byte-1", "byte-2", "byte-3", "byte-4"] {
        sqlx::query(
            "INSERT INTO broker_receipts
             (credential_hash, request_id, request_digest, response_json)
             VALUES (?, ?, ?, ?)",
        )
        .bind(credential.as_bytes())
        .bind(request_id)
        .bind([0_u8; 32].as_slice())
        .bind("x".repeat(10))
        .execute(&pool)
        .await
        .unwrap();
    }
    let mut transaction = pool.begin().await.unwrap();
    crate::store::prune_receipts_with_limits(&mut transaction, &credential, 64, 25, 100)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let retained: Vec<String> =
        sqlx::query_scalar("SELECT request_id FROM broker_receipts ORDER BY receipt_sequence")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(retained, ["byte-3", "byte-4"]);

    let mut transaction = pool.begin().await.unwrap();
    crate::store::prune_receipts_with_limits(&mut transaction, &credential, 64, 100, 15)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let retained: Vec<String> =
        sqlx::query_scalar("SELECT request_id FROM broker_receipts ORDER BY receipt_sequence")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(retained, ["byte-4"]);
    pool.close().await;
}
