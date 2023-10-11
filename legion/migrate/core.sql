drop schema if exists legion cascade;
create schema legion;

-- Create the tables
create table if not exists legion.job_queues (
  id int primary key generated always as identity,
  queue_name text not null unique check (length(queue_name) <= 128)
);

drop type if exists legion.job_status;
create type legion.job_status as enum ('pending', 'running', 'failed', 'completed');

create table if not exists legion.jobs (
    id bigint primary key generated always as identity,
    task_id text not null,
    job_queue_id int references legion.job_queues(id) on delete cascade,
    status legion.job_status not null default 'pending'::legion.job_status,
    payload json default '{}'::json not null,
    priority smallint default 0 not null,
    run_at timestamptz default now() not null,
    attempts smallint default 0 not null,
    max_attempts smallint default 25 not null not null constraint jobs_max_attempts_check check (max_attempts >= 1),
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index jobs_main_index on legion.jobs using btree (job_queue_id, status, attempts, priority);

-- Keep updated_at up to date
create or replace function 
    legion.tg__update_timestamp() 
returns trigger as $$ 
begin 
    new.updated_at = greatest(now(), old.updated_at + interval '1 millisecond');
return new;
end;
$$ language plpgsql;

create trigger 
    update_timestamp 
before update on 
    legion.jobs 
for each row execute procedure 
    legion.tg__update_timestamp();
    
-- Notify workers subscribed to a queue when a job is added or updated
create or replace function 
    legion.tg__notify_queue() 
returns trigger as $$ 
begin 
    perform pg_notify('legion_queue_' || new.job_queue_id, new.id::text);
return new;
end;
$$ language plpgsql;

create trigger 
    notify_queue
after insert or update on 
    legion.jobs 
for each row execute procedure 
    legion.tg__notify_queue();

-- Update failed job
create or replace function
    legion.fail_job(job_id legion.jobs.id%type, error_message text)
returns void as $$
declare 
    attempts smallint;
    max_attempts smallint;
begin

    -- set as pending if attempts < max_attempts
    select 
        attempts, max_attempts
    into 
        attempts, max_attempts
    from 
        legion.jobs
    where id = job_id;

    if attempts < max_attempts then
        update 
            legion.jobs
        set 
            status = 'pending',
            attempts = attempts + 1,
            last_error = error_message
        where 
            id = job_id;
        return;
    end if;

    -- set as failed if attempts == max_attempts
    update legion.jobs
    set status = 'failed',
        last_error = error_message
    where id = job_id;
end;
$$ language plpgsql;

-- Get next job to run
create or replace function
    legion.get_next_job(jq_id legion.job_queues.id%type)
returns 
    setof legion.jobs as $$
begin
    return query
        select 
            *
        from 
            legion.jobs
        where 
            job_queue_id = jq_id
            and status = 'pending'
        order by 
            priority desc,
            run_at asc
        for update skip locked
        limit 1;
end;
$$ language plpgsql;

-- Get or create a job queue by name
create or replace function
    legion.get_or_create_job_queue(name text)
returns
    legion.job_queues.id%type as $$
declare
    jq_id legion.job_queues.id%type;
begin
    select 
        id
    into
        jq_id
    from
        legion.job_queues
    where
        queue_name = name;


    if jq_id is null then
        insert into legion.job_queues(queue_name)
        values (name)
        returning id into jq_id;
    end if;

    return jq_id;
end;
$$ language plpgsql;