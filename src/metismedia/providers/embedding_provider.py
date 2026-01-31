"""Embedding provider interface for generating text embeddings."""

import hashlib
import math
from abc import ABC, abstractmethod
from typing import Protocol


class EmbeddingProvider(Protocol):
    """Protocol for generating text embeddings."""

    async def embed(
        self,
        texts: list[str],
        model: str | None = None,
    ) -> list[list[float]]:
        """Generate embeddings for a list of texts.

        Args:
            texts: List of text strings to embed
            model: Optional model name

        Returns:
            List of embedding vectors (same order as input texts)
        """
        ...


class MockEmbeddingProvider:
    """Mock implementation for testing without external API calls.

    Generates deterministic pseudo-embeddings based on text hash.
    """

    def __init__(
        self,
        dims: int = 1536,
        call_counter: dict[str, int] | None = None,
    ) -> None:
        """Initialize mock provider.

        Args:
            dims: Embedding dimensions
            call_counter: Optional dict to track call counts
        """
        self.dims = dims
        self._call_counter = call_counter if call_counter is not None else {}
        self._cached_embeddings: dict[str, list[float]] = {}

    def get_call_count(self) -> int:
        """Get total number of embed calls."""
        return self._call_counter.get("total", 0)

    def set_embedding_for_text(self, text: str, embedding: list[float]) -> None:
        """Set a specific embedding to return for a text."""
        self._cached_embeddings[text] = embedding

    async def embed(
        self,
        texts: list[str],
        model: str | None = None,
    ) -> list[list[float]]:
        """Generate deterministic pseudo-embeddings for texts."""
        self._call_counter["total"] = self._call_counter.get("total", 0) + 1

        results = []
        for text in texts:
            if text in self._cached_embeddings:
                results.append(self._cached_embeddings[text])
            else:
                results.append(self._generate_pseudo_embedding(text))

        return results

    def _generate_pseudo_embedding(self, text: str) -> list[float]:
        """Generate a deterministic pseudo-embedding from text hash.

        Creates a normalized vector based on MD5 hash of the text,
        ensuring consistent results for the same input.
        """
        hash_bytes = hashlib.md5(text.encode()).digest()

        embedding = []
        for i in range(self.dims):
            byte_idx = i % len(hash_bytes)
            seed_val = hash_bytes[byte_idx] + (i // len(hash_bytes)) * 256
            val = math.sin(seed_val * 0.1) * 0.5 + 0.5
            embedding.append(val - 0.5)

        norm = math.sqrt(sum(x * x for x in embedding))
        if norm > 0:
            embedding = [x / norm for x in embedding]

        return embedding


def cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    if len(vec1) != len(vec2):
        raise ValueError("Vectors must have same dimensions")

    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(a * a for a in vec1))
    norm2 = math.sqrt(sum(b * b for b in vec2))

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return dot_product / (norm1 * norm2)
