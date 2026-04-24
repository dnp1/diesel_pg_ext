-- Your SQL goes here
-- Enable PostgreSQL extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enum for status (optional, but type-safe)
CREATE TYPE post_status AS ENUM ('draft', 'published', 'archived');

-- Main posts table
CREATE TABLE posts
(
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   UUID        NOT NULL DEFAULT uuid_generate_v4(),
    category_id UUID        NOT NULL,

    title       TEXT        NOT NULL,
    tag         TEXT, -- Nullable for json_object_agg filtering demo
    content     TEXT,

    status      post_status NOT NULL DEFAULT 'draft',
    view_count  INTEGER     NOT NULL DEFAULT 0,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX idx_posts_tenant_category ON posts (tenant_id, category_id);
CREATE INDEX idx_posts_status_created ON posts (status, created_at DESC);
CREATE INDEX idx_posts_tag ON posts (tag) WHERE tag IS NOT NULL;

-- Auto-update updated_at trigger (optional but recommended)
CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_posts_updated_at
    BEFORE UPDATE
    ON posts
    FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();


-- ── Seed data for tenant-a, category-1 ──────────────────────────────────────
INSERT INTO posts (tenant_id, category_id, title, tag, status, view_count, created_at)
VALUES
    -- Published posts with tags (will appear in both aggregates)
    ('a1111111-1111-1111-1111-111111111111'::uuid, 'c1111111-1111-1111-1111-111111111111'::uuid,
     'Getting Started with Rust', 'rust', 'published', 150, '2024-01-15 10:00:00+00'),

    ('a1111111-1111-1111-1111-111111111111'::uuid, 'c1111111-1111-1111-1111-111111111111'::uuid,
     'Async Rust Patterns', 'rust', 'published', 320, '2024-01-16 14:30:00+00'),

    ('a1111111-1111-1111-1111-111111111111'::uuid, 'c1111111-1111-1111-1111-111111111111'::uuid,
     'Diesel ORM Deep Dive', 'rust', 'published', 89, '2024-01-17 09:15:00+00'),

    ('a1111111-1111-1111-1111-111111111111'::uuid, 'c1111111-1111-1111-1111-111111111111'::uuid,
     'PostgreSQL Window Functions', 'postgres', 'published', 210, '2024-01-18 16:45:00+00'),

    ('a1111111-1111-1111-1111-111111111111'::uuid, 'c1111111-1111-1111-1111-111111111111'::uuid,
     'OPAQUE Authentication Guide', 'security', 'published', 445, '2024-01-19 11:20:00+00'),

    -- Draft posts (filtered out by .filter(status.eq("published")))
    ('a1111111-1111-1111-1111-111111111111'::uuid, 'c1111111-1111-1111-1111-111111111111'::uuid,
     'WIP: Macro Magic', NULL, 'draft', 12, '2024-01-20 08:00:00+00'),

    -- Post with NULL tag (filtered out of json_object_agg by .filter(tag.is_not_null()))
    ('a1111111-1111-1111-1111-111111111111'::uuid, 'c1111111-1111-1111-1111-111111111111'::uuid,
     'Untitled Draft', NULL, 'published', 5, '2024-01-21 13:00:00+00'),

    -- Duplicate title to test .distinct() in array_agg
    ('a1111111-1111-1111-1111-111111111111'::uuid, 'c1111111-1111-1111-1111-111111111111'::uuid,
     'Getting Started with Rust', 'beginner', 'published', 78, '2024-01-22 10:30:00+00');


-- ── Seed data for tenant-a, category-2 (different partition for window demo) ─
INSERT INTO posts (tenant_id, category_id, title, tag, status, view_count, created_at)
VALUES ('a1111111-1111-1111-1111-111111111111'::uuid, 'c2222222-2222-2222-2222-222222222222'::uuid,
        'Category 2 Post A', 'tutorial', 'published', 55, '2024-01-15 12:00:00+00'),

       ('a1111111-1111-1111-1111-111111111111'::uuid, 'c2222222-2222-2222-2222-222222222222'::uuid,
        'Category 2 Post B', 'guide', 'published', 120, '2024-01-16 15:00:00+00');


-- ── Seed data for tenant-b (different tenant for GROUP BY demo) ─────────────
INSERT INTO posts (tenant_id, category_id, title, tag, status, view_count, created_at)
VALUES ('b2222222-2222-2222-2222-222222222222'::uuid, 'c1111111-1111-1111-1111-111111111111'::uuid,
        'Tenant B Exclusive', 'exclusive', 'published', 300, '2024-01-17 10:00:00+00'),

       ('b2222222-2222-2222-2222-222222222222'::uuid, 'c1111111-1111-1111-1111-111111111111'::uuid,
        'Another B Post', 'news', 'published', 180, '2024-01-18 14:00:00+00');
