"""
Module 2 v1.2.0: Unit tests for Kafka producer 3-topic routing.
Owner: Karthik
"""
import pytest

from producer.kafka_producer import (
    TOPIC_FLUIDS,
    TOPIC_LABS,
    TOPIC_VITALS,
    topic_for,
)


@pytest.mark.parametrize(
    "event_type, expected",
    [
        ("creatinine", TOPIC_LABS),
        ("bmp", TOPIC_LABS),
        ("electrolytes", TOPIC_LABS),
        ("lab", TOPIC_LABS),
        ("heart_rate", TOPIC_VITALS),
        ("blood_pressure", TOPIC_VITALS),
        ("spo2", TOPIC_VITALS),
        ("temperature", TOPIC_VITALS),
        ("vital", TOPIC_VITALS),
        ("urine", TOPIC_FLUIDS),
        ("urine_output", TOPIC_FLUIDS),
        ("fluid", TOPIC_FLUIDS),
        ("iv_total", TOPIC_FLUIDS),
    ],
)
def test_topic_routing_v120(event_type, expected):
    """Every v1.2.0 event_type lands on the right topic."""
    assert topic_for(event_type) == expected


def test_topic_routing_is_case_insensitive():
    assert topic_for("CREATININE") == TOPIC_LABS
    assert topic_for(" Heart_Rate ") == TOPIC_VITALS


def test_topic_routing_unknown_returns_none():
    assert topic_for("ekg") is None
    assert topic_for("") is None
    assert topic_for(None) is None


def test_topics_are_distinct():
    """Sanity: the three v1.2.0 topics are not aliased to the same name."""
    assert len({TOPIC_LABS, TOPIC_VITALS, TOPIC_FLUIDS}) == 3
