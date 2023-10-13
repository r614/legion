use log::error;
use sqlx::Executor;

pub async fn migrate(url: String) {
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    match (&pool).execute(include_str!("migrations/schema.sql")).await {
        Ok(_) => println!("Schema created"),
        Err(e) => error!("Error creating schema: {:?}", e),
    }
}
