"""Smoke tests for database schema and extensions."""

import os
import uuid

import psycopg2
import pytest
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/metismedia")


@pytest.fixture
def db_connection():
    """Create a database connection for testing."""
    conn = psycopg2.connect(DATABASE_URL)
    yield conn
    conn.rollback()
    conn.close()


def test_vector_extension_exists(db_connection):
    """Assert vector extension is installed."""
    cursor = db_connection.cursor()
    cursor.execute("SELECT extname FROM pg_extension WHERE extname = 'vector'")
    result = cursor.fetchone()
    assert result is not None, "vector extension not found"
    assert result[0] == "vector"


def test_pg_trgm_extension_exists(db_connection):
    """Assert pg_trgm extension is installed."""
    cursor = db_connection.cursor()
    cursor.execute("SELECT extname FROM pg_extension WHERE extname = 'pg_trgm'")
    result = cursor.fetchone()
    assert result is not None, "pg_trgm extension not found"
    assert result[0] == "pg_trgm"


def test_citext_extension_exists(db_connection):
    """Assert citext extension is installed."""
    cursor = db_connection.cursor()
    cursor.execute("SELECT extname FROM pg_extension WHERE extname = 'citext'")
    result = cursor.fetchone()
    assert result is not None, "citext extension not found"
    assert result[0] == "citext"


EXPECTED_TABLES = [
    "campaigns",
    "embeddings",
    "influencers",
    "influencer_platforms",
    "receipts",
    "target_cards",
    "contact_methods",
    "drafts",
    "pitch_events",
    "reservations",
]


@pytest.mark.parametrize("table_name", EXPECTED_TABLES)
def test_table_exists(db_connection, table_name):
    """Assert expected tables exist."""
    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s",
        (table_name,),
    )
    result = cursor.fetchone()
    assert result is not None, f"Table {table_name} not found"


def test_insert_campaign(db_connection):
    """Insert a minimal campaign row."""
    cursor = db_connection.cursor()
    campaign_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    cursor.execute(
        """
        INSERT INTO campaigns (id, tenant_id, brief)
        VALUES (%s, %s, %s)
        RETURNING id
        """,
        (str(campaign_id), str(tenant_id), '{"name": "test"}'),
    )
    result = cursor.fetchone()
    assert result is not None
    assert str(result[0]) == str(campaign_id)


def test_insert_influencer(db_connection):
    """Insert a minimal influencer row."""
    cursor = db_connection.cursor()
    influencer_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    cursor.execute(
        """
        INSERT INTO influencers (id, tenant_id, canonical_name)
        VALUES (%s, %s, %s)
        RETURNING id
        """,
        (str(influencer_id), str(tenant_id), "Test Influencer"),
    )
    result = cursor.fetchone()
    assert result is not None
    assert str(result[0]) == str(influencer_id)


def test_insert_embedding_with_vector(db_connection):
    """Insert an embedding with a vector."""
    cursor = db_connection.cursor()
    embedding_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    vector_data = [0.1] * 1536

    cursor.execute(
        """
        INSERT INTO embeddings (id, tenant_id, kind, embedding_model, embedding_dims, vector)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (str(embedding_id), str(tenant_id), "bio", "text-embedding-3-small", 1536, str(vector_data)),
    )
    result = cursor.fetchone()
    assert result is not None
    assert str(result[0]) == str(embedding_id)


def test_tenant_url_uniqueness(db_connection):
    """Test tenant-scoped uniqueness on influencers.primary_url."""
    cursor = db_connection.cursor()
    tenant_id = uuid.uuid4()
    url = "https://example.com/test"

    cursor.execute(
        """
        INSERT INTO influencers (id, tenant_id, canonical_name, primary_url)
        VALUES (%s, %s, %s, %s)
        """,
        (str(uuid.uuid4()), str(tenant_id), "First", url),
    )

    with pytest.raises(psycopg2.errors.UniqueViolation):
        cursor.execute(
            """
            INSERT INTO influencers (id, tenant_id, canonical_name, primary_url)
            VALUES (%s, %s, %s, %s)
            """,
            (str(uuid.uuid4()), str(tenant_id), "Second", url),
        )
