use diesel_migrations::{EmbeddedMigrations, embed_migrations};
use test_containers_util::diesel_pg::PostgresTestDb;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("tests/migrations");

pub async fn get_db() -> PostgresTestDb {
    PostgresTestDb::create("diesel_pg_ext_test", MIGRATIONS, Some("public"), None).await
}
