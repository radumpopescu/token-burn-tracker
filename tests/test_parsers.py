from __future__ import annotations

import unittest

from token_burn.parsers import parse_claude_usage_json, parse_codex_usage_json


class ParserTests(unittest.TestCase):
    def test_parse_claude_usage_json(self) -> None:
        payload = {
            "five_hour": {
                "utilization": 11.0,
                "resets_at": "2026-03-22T03:00:00.458696+00:00",
            },
            "seven_day": {
                "utilization": 17.0,
                "resets_at": "2026-03-24T13:00:00.458712+00:00",
            },
            "extra_usage": {
                "is_enabled": False,
                "monthly_limit": None,
                "used_credits": None,
                "utilization": None,
            },
        }

        snapshot = parse_claude_usage_json(payload, "2026-03-22T00:00:00+00:00")

        self.assertEqual(snapshot.provider, "claude")
        self.assertIn("5 hour: 11.0%", snapshot.summary)
        self.assertEqual(len(snapshot.metrics), 2)
        self.assertEqual(snapshot.metrics[0].key, "five_hour_utilization")
        self.assertEqual(snapshot.metrics[1].percent_value, 17.0)

    def test_parse_codex_usage_json(self) -> None:
        payload = {
            "user_id": "user-abc",
            "account_id": "user-abc",
            "email": "hidden@example.com",
            "plan_type": "pro",
            "rate_limit": {
                "primary_window": {
                    "used_percent": 6,
                    "limit_window_seconds": 18000,
                    "reset_after_seconds": 15574,
                    "reset_at": 1774152248,
                },
                "secondary_window": {
                    "used_percent": 34,
                    "limit_window_seconds": 604800,
                    "reset_after_seconds": 247436,
                    "reset_at": 1774384110,
                },
            },
            "code_review_rate_limit": {
                "primary_window": {
                    "used_percent": 0,
                    "limit_window_seconds": 604800,
                    "reset_after_seconds": 604800,
                    "reset_at": 1774741475,
                },
                "secondary_window": None,
            },
            "additional_rate_limits": [
                {
                    "limit_name": "GPT-5.3-Codex-Spark",
                    "metered_feature": "codex_bengalfox",
                    "rate_limit": {
                        "primary_window": {
                            "used_percent": 0,
                            "limit_window_seconds": 18000,
                            "reset_after_seconds": 18000,
                            "reset_at": 1774154675,
                        },
                        "secondary_window": {
                            "used_percent": 13,
                            "limit_window_seconds": 604800,
                            "reset_after_seconds": 250122,
                            "reset_at": 1774386796,
                        },
                    },
                }
            ],
            "credits": {
                "has_credits": False,
                "unlimited": False,
                "balance": "0",
                "approx_local_messages": [0, 0],
                "approx_cloud_messages": [0, 0],
            },
        }

        snapshot = parse_codex_usage_json(payload, "2026-03-22T00:00:00+00:00")

        self.assertEqual(snapshot.provider, "codex")
        self.assertEqual(snapshot.plan_name, "Pro")
        self.assertIn("Primary window: 6.0%", snapshot.summary)
        self.assertNotIn("hidden@example.com", snapshot.raw_text)
        metric_keys = {metric.key for metric in snapshot.metrics}
        self.assertIn("primary_window", metric_keys)
        self.assertIn("secondary_window", metric_keys)
        self.assertIn("gpt_5_3_codex_spark_secondary_window", metric_keys)


if __name__ == "__main__":
    unittest.main()
