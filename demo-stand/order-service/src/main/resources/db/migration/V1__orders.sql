create table orders (
    id uuid primary key, customer_id text not null, total numeric(12,2) not null,
    created_at timestamptz not null default now()
);
