import os
import uuid

import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://image_resize:image_resize@db:5432/image_resize",
)


def get_connection():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """Create the image_jobs table if it does not exist."""
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS image_jobs (
                    id UUID PRIMARY KEY,
                    filename TEXT NOT NULL,
                    original_filename TEXT,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_image_jobs_filename
                ON image_jobs (filename);
                """
            )
    finally:
        conn.close()


def create_job(
    filename: str,
    original_filename: str | None = None,
    job_id: str | None = None,
) -> str:
    """Insert a new image job with status 'pending' and return its UUID."""
    if job_id is None:
        job_id = str(uuid.uuid4())
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO image_jobs (id, filename, original_filename, status)
                VALUES (%s, %s, %s, 'pending');
                """,
                (job_id, filename, original_filename),
            )
    finally:
        conn.close()
    return job_id


def update_job_status(job_id: str, status: str, error_message: str | None = None) -> None:
    """Update the status (and optional error message) for a job."""
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE image_jobs
                SET status = %s,
                    error_message = %s,
                    updated_at = NOW()
                WHERE id = %s;
                """,
                (status, error_message, job_id),
            )
    finally:
        conn.close()


def get_job(job_id: str) -> dict | None:
    """Fetch a job record by ID."""
    conn = get_connection()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, filename, original_filename, status, error_message,
                       created_at, updated_at
                FROM image_jobs
                WHERE id = %s;
                """,
                (job_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()

