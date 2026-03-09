create table if not exists pull_requests (
    id bigint primary key,
    number integer not null unique,
    title text,
    author text not null,
    state text,
    merged boolean default false,
    created_at timestamptz,
    merged_at timestamptz,
    additions integer default 0,
    deletions integer default 0,
    changed_files integer default 0
);

create table if not exists reviews (
    id bigint primary key,
    pull_request_number integer not null references pull_requests(number),
    reviewer text not null,
    state text not null,
    submitted_at timestamptz
);

create index if not exists idx_pr_author on pull_requests(author);
create index if not exists idx_pr_merged on pull_requests(merged);
create index if not exists idx_reviews_reviewer on reviews(reviewer);
create index if not exists idx_reviews_state on reviews(state);
create index if not exists idx_reviews_pr_number on reviews(pull_request_number);
