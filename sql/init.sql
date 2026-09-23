-- Run once with a migration role; application role needs SELECT/INSERT/UPDATE only.
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS rag_documents (
  source_id text PRIMARY KEY,
  product text NOT NULL,
  content text NOT NULL,
  embedding vector(1024) NOT NULL,
  updated_at timestamptz NOT NULL,
  batch_id text NOT NULL
);
CREATE INDEX IF NOT EXISTS rag_product ON rag_documents(product);
CREATE INDEX IF NOT EXISTS rag_embedding ON rag_documents USING hnsw (embedding vector_cosine_ops);
