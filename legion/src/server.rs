use duct::cmd;
use std::{error::Error, fmt::Debug};

use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone, sqlx::FromRow)]
pub struct JobQueue {
    pub id: i64,
    pub queue_name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, sqlx::FromRow)]
pub struct Job {
    pub id: i64,
    pub payload: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone, sqlx::Type)]
pub enum Status {
    Pending,
    Running,
    Failed,
    Completed,
}

async fn get_job(
    queue_name: String,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<Option<Job>, sqlx::Error> {
    let row = sqlx::query_as::<_, Job>("select id, payload from legion.get_next_job($1)")
        .bind(queue_name)
        .fetch_optional(&mut **tx)
        .await?;

    Ok(row)
}

async fn set_complete(
    job: Job,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    let res = sqlx::query(
        r"
                        update 
                            legion.jobs
                        set
                            status = 'completed'
                        where
                            id = $1
                    ",
    )
    .bind(job.id)
    .execute(&mut **tx)
    .await?;

    match res.rows_affected() {
        1 => Ok(()),
        _ => Err(sqlx::Error::RowNotFound),
    }
}

async fn fail_job(
    job: Job,
    error: String,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    let res = sqlx::query(
        r"
            select 
                *
            from
                legion.fail_job($1, $2)
        ",
    )
    .bind(job.id)
    .bind(error)
    .execute(&mut **tx)
    .await?;

    match res.rows_affected() {
        1 => Ok(()),
        _ => Err(sqlx::Error::RowNotFound),
    }
}

async fn do_work(
    queue_name: String,
    command: String,
    pool: &sqlx::PgPool,
) -> Result<(), Box<dyn Error>> {
    let mut transaction = pool.begin().await?;
    let job = get_job(queue_name, &mut transaction).await;
    transaction.commit().await?;

    match job {
        Ok(Some(job)) => {
            info!("Got job: {:?}", job.id);

            let c = cmd!("bash", "-c", command)
                .pipe(cmd!(
                    "tee",
                    "-a",
                    format!("/tmp/legion_{}.log", job.id.to_string())
                ))
                .run();

            match c {
                Ok(output) => {
                    info!(
                        "Command output: \n{}",
                        String::from_utf8_lossy(&output.stdout)
                    );

                    let mut tx = pool.begin().await?;
                    let res = set_complete(job, &mut tx).await;
                    tx.commit().await?;
                    return {
                        match res {
                            Ok(_) => Ok(()),
                            Err(e) => {
                                error!("Error setting job to complete: {:?}", e);
                                Err(Box::new(e))
                            }
                        }
                    };
                }
                Err(e) => {
                    let logs =
                        std::fs::read_to_string(format!("/tmp/legion_{}.log", job.id.to_string()))?;

                    error!("Error running command: {:?}\nCommand Output: \n{}", e, logs);

                    let mut tx = pool.begin().await?;
                    let res = fail_job(job, format!("{:?}", logs), &mut tx).await;
                    tx.commit().await?;
                    return {
                        match res {
                            Ok(_) => Ok(()),
                            Err(e) => {
                                error!("Error setting job to failed: {:?}", e);
                                Err(Box::new(e))
                            }
                        }
                    };
                }
            }
        }
        Ok(None) => {
            info!("No job found");
            Ok(())
        }
        Err(e) => {
            error!("Error getting job: {:?}", e);
            Err(Box::new(e))
        }
    }
}

pub async fn start(url: String, queue: String, command: String) -> Result<(), Box<dyn Error>> {
    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("Failed to connect to postgres");

    // listen to postgres for notifications
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
        .await
        .expect("Failed to connect to postgres");

    let legion_queue = format!("legion_queue_{}", queue);

    listener
        .listen(&legion_queue)
        .await
        .expect("Failed to listen to postgres");

    info!(
        "Listening to postgres for notifications on {}",
        legion_queue
    );
    loop {
        do_work(queue.clone(), command.clone(), &pool).await?;
        let _ = listener.recv().await?;
    }
}
