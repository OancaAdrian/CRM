-- migrations/001_create_trgm_index.sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS firms_denumire_trgm_idx
  ON public.firms USING gin (lower(left(denumire,60)) gin_trgm_ops);

ANALYZE public.firms;
