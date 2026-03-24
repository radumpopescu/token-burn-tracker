from datetime import datetime
import unittest

from token_burn.web import _dashboard_provider_order, _resolve_range


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


if __name__ == "__main__":
    unittest.main()
