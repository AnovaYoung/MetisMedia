"""Node A provider interface for slot extraction and question generation."""

import re
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field

from metismedia.settings import get_settings


SLOT_SCHEMA = {
    "strategic_intent": {
        "description": "High-level campaign goal (e.g., 'brand awareness', 'thought leadership')",
        "required": False,
        "hard_blocker": False,
    },
    "polarity_intent": {
        "description": "Whether targeting allies, critics, or watchlist",
        "required": True,
        "hard_blocker": True,
        "valid_values": ["allies", "critics", "watchlist"],
    },
    "platform_vector": {
        "description": "Target platforms (e.g., ['x', 'substack', 'youtube'])",
        "required": True,
        "hard_blocker": True,
        "valid_values": ["x", "bluesky", "substack", "blog", "newsletter", "podcast", "youtube", "reddit", "other"],
    },
    "geography": {
        "description": "Geographic focus (e.g., 'US', 'EU', 'global')",
        "required": True,
        "hard_blocker": True,
    },
    "influence_tier": {
        "description": "Follower range tier (e.g., 'micro', 'mid', 'macro')",
        "required": False,
        "hard_blocker": False,
    },
    "vibe": {
        "description": "Psychographic/tone descriptors",
        "required": False,
        "hard_blocker": False,
    },
    "third_rail_terms": {
        "description": "Topics to avoid",
        "required": False,
        "hard_blocker": False,
    },
    "receipts_offered": {
        "description": "Whether client provides existing target URLs",
        "required": False,
        "hard_blocker": False,
    },
    "commercial_mode": {
        "description": "Engagement mode: earned, paid, hybrid, or unknown",
        "required": True,
        "hard_blocker": True,
        "valid_values": ["earned", "paid", "hybrid", "unknown"],
    },
    "campaign_name": {
        "description": "Name for the campaign",
        "required": True,
        "hard_blocker": False,
    },
    "campaign_description": {
        "description": "Brief description of campaign goals",
        "required": True,
        "hard_blocker": False,
    },
}

HARD_BLOCKER_SLOTS = [
    slot for slot, schema in SLOT_SCHEMA.items()
    if schema.get("hard_blocker", False)
]

REQUIRED_SLOTS = [
    slot for slot, schema in SLOT_SCHEMA.items()
    if schema.get("required", False)
]


class SlotExtractionResult(BaseModel):
    """Result of slot extraction from a message."""

    updated_slots: dict[str, Any] = Field(default_factory=dict)
    confidences: dict[str, float] = Field(default_factory=dict)
    extracted_count: int = 0

    model_config = {"extra": "forbid"}


class NodeAProvider(ABC):
    """Abstract interface for Node A slot extraction and question generation."""

    @abstractmethod
    async def extract_slots(
        self,
        message: str,
        current_slots: dict[str, Any],
    ) -> SlotExtractionResult:
        """Extract slot values from user message.

        Args:
            message: User's input message
            current_slots: Currently filled slots

        Returns:
            SlotExtractionResult with updated slots and confidences
        """
        ...

    @abstractmethod
    async def suggest_next_question(
        self,
        current_slots: dict[str, Any],
        missing_slots: list[str],
        confidences: dict[str, float],
    ) -> str:
        """Suggest the next clarification question.

        Args:
            current_slots: Currently filled slots
            missing_slots: Slots that still need values
            confidences: Confidence scores for filled slots

        Returns:
            Question string to ask the user
        """
        ...


def compute_missing_slots(
    slots: dict[str, Any],
    confidences: dict[str, float],
) -> list[str]:
    """Compute which required slots are still missing or below threshold."""
    threshold = get_settings().node_a_hardblocker_min_confidence
    missing = []

    for slot_name in REQUIRED_SLOTS:
        value = slots.get(slot_name)

        if value is None or value == "" or value == []:
            missing.append(slot_name)
            continue

        if slot_name in HARD_BLOCKER_SLOTS:
            confidence = confidences.get(slot_name, 0.0)
            if confidence < threshold:
                missing.append(slot_name)

    return missing


def is_ready_to_finalize(
    slots: dict[str, Any],
    confidences: dict[str, float],
) -> tuple[bool, list[str]]:
    """Check if session is ready to finalize.

    Returns:
        (is_ready, list of blocking slots)
    """
    threshold = get_settings().node_a_hardblocker_min_confidence
    blocking = []

    for slot_name in HARD_BLOCKER_SLOTS:
        value = slots.get(slot_name)
        if value is None or value == "" or value == []:
            blocking.append(slot_name)
            continue

        confidence = confidences.get(slot_name, 0.0)
        if confidence < threshold:
            blocking.append(slot_name)

    for slot_name in ["campaign_name", "campaign_description"]:
        value = slots.get(slot_name)
        if not value:
            blocking.append(slot_name)

    return len(blocking) == 0, blocking


class MockNodeAProvider(NodeAProvider):
    """Mock implementation for testing without LLM calls."""

    KEYWORD_MAPPINGS = {
        "polarity_intent": {
            "allies": ["allies", "friendly", "supporters", "advocates", "fans"],
            "critics": ["critics", "skeptics", "negative", "opposition"],
            "watchlist": ["watchlist", "monitor", "watch", "track"],
        },
        "commercial_mode": {
            "earned": ["earned", "organic", "free", "unpaid"],
            "paid": ["paid", "sponsored", "budget", "compensate"],
            "hybrid": ["hybrid", "mix", "both"],
        },
        "platform_vector": {
            "x": ["twitter", "x.com", " x "],
            "substack": ["substack", "newsletter"],
            "youtube": ["youtube", "video"],
            "podcast": ["podcast", "audio"],
            "bluesky": ["bluesky", "bsky"],
            "blog": ["blog", "website"],
        },
    }

    GEOGRAPHY_PATTERNS = [
        (r"\b(us|usa|united states|america)\b", "US"),
        (r"\b(uk|united kingdom|britain)\b", "UK"),
        (r"\b(eu|europe|european)\b", "EU"),
        (r"\b(global|worldwide|international)\b", "global"),
        (r"\b(apac|asia)\b", "APAC"),
    ]

    async def extract_slots(
        self,
        message: str,
        current_slots: dict[str, Any],
    ) -> SlotExtractionResult:
        """Extract slots using keyword matching (mock implementation)."""
        updated_slots = current_slots.copy()
        confidences: dict[str, float] = {}
        extracted_count = 0
        message_lower = message.lower()

        for slot_name, value_keywords in self.KEYWORD_MAPPINGS.items():
            for value, keywords in value_keywords.items():
                for keyword in keywords:
                    if keyword in message_lower:
                        if slot_name == "platform_vector":
                            existing = updated_slots.get("platform_vector", [])
                            if isinstance(existing, str):
                                existing = [existing]
                            if value not in existing:
                                existing.append(value)
                                updated_slots["platform_vector"] = existing
                                confidences["platform_vector"] = 0.85
                                extracted_count += 1
                        else:
                            if updated_slots.get(slot_name) != value:
                                updated_slots[slot_name] = value
                                confidences[slot_name] = 0.9
                                extracted_count += 1
                        break

        for pattern, geo_value in self.GEOGRAPHY_PATTERNS:
            if re.search(pattern, message_lower):
                if updated_slots.get("geography") != geo_value:
                    updated_slots["geography"] = geo_value
                    confidences["geography"] = 0.85
                    extracted_count += 1
                break

        name_match = re.search(
            r"(?:campaign|project|initiative)\s+(?:called|named|is)\s+[\"']?([^\"'\n]+)[\"']?",
            message_lower,
        )
        if name_match:
            name = name_match.group(1).strip().title()
            if updated_slots.get("campaign_name") != name:
                updated_slots["campaign_name"] = name
                confidences["campaign_name"] = 0.8
                extracted_count += 1

        if "name:" in message_lower or "campaign name:" in message_lower:
            parts = re.split(r"name:\s*", message_lower, maxsplit=1)
            if len(parts) > 1:
                name = parts[1].split("\n")[0].strip().title()
                if name and updated_slots.get("campaign_name") != name:
                    updated_slots["campaign_name"] = name
                    confidences["campaign_name"] = 0.85
                    extracted_count += 1

        if len(message) > 50 and not updated_slots.get("campaign_description"):
            updated_slots["campaign_description"] = message[:200]
            confidences["campaign_description"] = 0.7
            extracted_count += 1

        tier_patterns = [
            (r"\b(micro|small)\b", "micro"),
            (r"\b(mid|medium)\b", "mid"),
            (r"\b(macro|large|big)\b", "macro"),
        ]
        for pattern, tier in tier_patterns:
            if re.search(pattern, message_lower):
                if updated_slots.get("influence_tier") != tier:
                    updated_slots["influence_tier"] = tier
                    confidences["influence_tier"] = 0.75
                    extracted_count += 1
                break

        return SlotExtractionResult(
            updated_slots=updated_slots,
            confidences=confidences,
            extracted_count=extracted_count,
        )

    async def suggest_next_question(
        self,
        current_slots: dict[str, Any],
        missing_slots: list[str],
        confidences: dict[str, float],
    ) -> str:
        """Generate next question based on missing slots."""
        if not missing_slots:
            return "All required information has been collected. Ready to finalize?"

        threshold = get_settings().node_a_hardblocker_min_confidence
        low_confidence_hard_blockers = [
            s for s in HARD_BLOCKER_SLOTS
            if s in current_slots
            and current_slots.get(s)
            and confidences.get(s, 0) < threshold
        ]

        if low_confidence_hard_blockers:
            slot = low_confidence_hard_blockers[0]
            current_value = current_slots.get(slot)
            return self._clarification_question(slot, current_value)

        priority_order = [
            "polarity_intent",
            "commercial_mode",
            "platform_vector",
            "geography",
            "campaign_name",
            "campaign_description",
            "influence_tier",
            "vibe",
        ]

        for slot in priority_order:
            if slot in missing_slots:
                return self._slot_question(slot)

        return self._slot_question(missing_slots[0])

    def _slot_question(self, slot: str) -> str:
        """Generate a question for a missing slot."""
        questions = {
            "polarity_intent": "Are you looking to engage with allies (supporters), critics, or create a watchlist?",
            "commercial_mode": "Will this be an earned media campaign, paid/sponsored, or a hybrid approach?",
            "platform_vector": "Which platforms should we focus on? (e.g., X/Twitter, Substack, YouTube, podcasts)",
            "geography": "What geographic region should we target? (e.g., US, EU, global)",
            "campaign_name": "What would you like to name this campaign?",
            "campaign_description": "Can you briefly describe the goals of this campaign?",
            "influence_tier": "What influencer size are you targeting? (micro, mid-tier, or macro)",
            "vibe": "What tone or vibe are you looking for in the influencers?",
            "third_rail_terms": "Are there any topics or terms we should avoid?",
            "strategic_intent": "What is the high-level strategic goal of this campaign?",
            "receipts_offered": "Do you have existing URLs or examples of target influencers to provide?",
        }
        return questions.get(slot, f"Please provide a value for: {slot}")

    def _clarification_question(self, slot: str, current_value: Any) -> str:
        """Generate a clarification question for low-confidence slot."""
        return f"I understood '{current_value}' for {slot.replace('_', ' ')}. Is that correct?"
