from datetime import datetime
import unittest

from token_burn.web import _dashboard_provider_order, _refresh_settings, _resolve_range


class WebTests(unittest.TestCase):
    def test_resolve_range_supports_one_hour_period(self) -> None:
        filters = _resolve_range(period="1h", start=None, end=None)

        self.assertEqual(filters["period"], "1h")
        self.assertIsNotNone(filters["start_at"])
        self.assertIsNotNone(filters["end_at"])

        start_at = datetime.fromisoformat(filters["start_at"])
        end_at = datetime.fromisoformat(filters["end_at"])

        self.assertTrue(3599 <= (end_at - start_at).total_seconds() <= 3601)

    def test_dashboard_provider_order_defaults_to_codex_first(self) -> None:
        self.assertEqual(_dashboard_provider_order({}), ["codex", "claude"])

    def test_dashboard_provider_order_respects_saved_top_provider(self) -> None:
        self.assertEqual(
            _dashboard_provider_order({"dashboard_top_provider": "claude"}),
            ["claude", "codex"],
        )

    def test_refresh_settings_defaults_match_current_behavior(self) -> None:
        settings = _refresh_settings({})

        self.assertEqual(settings["fast_interval_seconds"], 60)
        self.assertEqual(settings["slow_interval_seconds"], 600)
        self.assertEqual(settings["auto_step_seconds"], 60)
        self.assertEqual(settings["equal_polls_before_step"], 10)
        self.assertEqual(settings["fast_label"], "1m")
        self.assertEqual(settings["slow_label"], "10m")

    def test_refresh_settings_clamp_slow_interval_to_fast_interval(self) -> None:
        settings = _refresh_settings(
            {
                "poll_interval_seconds": "180",
                "slow_refresh_interval_seconds": "60",
                "auto_refresh_step_seconds": "120",
                "auto_refresh_equal_polls_before_step": "3",
            }
        )

        self.assertEqual(settings["fast_interval_seconds"], 180)
        self.assertEqual(settings["slow_interval_seconds"], 180)
        self.assertEqual(settings["auto_step_seconds"], 120)
        self.assertEqual(settings["equal_polls_before_step"], 3)
        self.assertEqual(settings["fast_label"], "3m")


if __name__ == "__main__":
    unittest.main()
