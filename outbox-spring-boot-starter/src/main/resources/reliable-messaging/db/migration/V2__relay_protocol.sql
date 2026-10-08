alter table outbox_message add column claim_token uuid;
create index outbox_dead_idx on outbox_message (created_at) where status = 'DEAD';
create index outbox_sent_idx on outbox_message (sent_at) where status = 'SENT';
create index inbox_processed_at_idx on inbox_message (processed_at);
