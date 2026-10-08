-- Deliberately no uniqueness on order_id: duplicate business effects must remain visible in the demo.
create table payments (
    id bigserial primary key, order_id uuid not null, amount numeric(12,2),
    created_at timestamptz not null default now()
);
create index payments_order_idx on payments(order_id);
create table delivery_log (
    message_id uuid not null, received_at timestamptz not null default now()
);
