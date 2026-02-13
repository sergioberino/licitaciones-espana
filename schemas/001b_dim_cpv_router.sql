CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS dim.cpv_router (
  num_code  INTEGER PRIMARY KEY REFERENCES dim.cpv_dim(num_code),
  embedding vector(1024) NOT NULL,
  label     TEXT
);

CREATE INDEX IF NOT EXISTS idx_cpv_router_embedding_hnsw
  ON dim.cpv_router USING hnsw (embedding vector_l2_ops);
