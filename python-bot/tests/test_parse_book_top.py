"""Unit tests for parse_book_top — the fix for the 2026-04-13 bad-fill incident."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from bot import parse_book_top


class TestParseBookTopFpSchema(unittest.TestCase):
    """Tests against the current Kalshi orderbook_fp schema (dollar strings)."""

    def test_full_no_side_with_empty_yes(self):
        """Real observed shape: deep NO book, empty YES book → yes_ask = 100 - best_no_bid."""
        book = {
            "orderbook_fp": {
                "no_dollars": [
                    ["0.5600", "1.00"],
                    ["0.7000", "21.00"],
                    ["0.8000", "1019.00"],
                    ["0.9000", "1017.00"],
                    ["0.9500", "1016.00"],
                    ["0.9600", "5000.00"],
                    ["0.9700", "1000.00"],
                    ["0.9800", "1402.00"],
                    ["0.9900", "9130.00"],
                ],
                "yes_dollars": [],
            }
        }
        yes_bid, yes_ask = parse_book_top(book)
        self.assertIsNone(yes_bid, "empty yes_dollars should give yes_bid=None")
        self.assertEqual(yes_ask, 1, "best no_bid 0.99 → yes_ask = 100 - 99 = 1")

    def test_full_yes_side_with_empty_no(self):
        book = {
            "orderbook_fp": {
                "yes_dollars": [
                    ["0.1000", "10.00"],
                    ["0.5000", "20.00"],
                    ["0.9500", "50.00"],
                ],
                "no_dollars": [],
            }
        }
        yes_bid, yes_ask = parse_book_top(book)
        self.assertEqual(yes_bid, 95, "best yes_bid is last element (ascending)")
        self.assertIsNone(yes_ask)

    def test_both_sides_populated(self):
        book = {
            "orderbook_fp": {
                "yes_dollars": [["0.3500", "10.00"], ["0.4000", "20.00"]],
                "no_dollars": [["0.5500", "10.00"], ["0.6000", "20.00"]],
            }
        }
        yes_bid, yes_ask = parse_book_top(book)
        self.assertEqual(yes_bid, 40)
        self.assertEqual(yes_ask, 100 - 60)  # 40

    def test_both_empty(self):
        book = {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}
        self.assertEqual(parse_book_top(book), (None, None))

    def test_missing_orderbook_fp(self):
        self.assertEqual(parse_book_top({}), (None, None))

    def test_null_lists(self):
        book = {"orderbook_fp": {"yes_dollars": None, "no_dollars": None}}
        self.assertEqual(parse_book_top(book), (None, None))

    def test_dollar_string_rounding(self):
        """0.9850 → 98 cents (rounded)."""
        book = {
            "orderbook_fp": {
                "yes_dollars": [["0.9850", "1.00"]],
                "no_dollars": [["0.0150", "1.00"]],
            }
        }
        yes_bid, yes_ask = parse_book_top(book)
        self.assertEqual(yes_bid, 98)
        # 0.015 * 100 = 1.5 → round to 2, 100 - 2 = 98
        self.assertEqual(yes_ask, 98)

    def test_malformed_levels_skipped(self):
        book = {
            "orderbook_fp": {
                "yes_dollars": [["not-a-number", "10"], ["0.5000", "20"]],
                "no_dollars": [["0.9500", "10"]],
            }
        }
        yes_bid, yes_ask = parse_book_top(book)
        self.assertEqual(yes_bid, 50, "malformed level skipped, best valid is 0.50")
        self.assertEqual(yes_ask, 5)


class TestParseBookTopLegacySchema(unittest.TestCase):
    """Tests against the legacy `orderbook` schema (cent integers)."""

    def test_legacy_both_sides(self):
        book = {
            "orderbook": {
                "yes": [[3, 100], [4, 50]],
                "no": [[95, 10], [97, 20]],
            }
        }
        yes_bid, yes_ask = parse_book_top(book)
        self.assertEqual(yes_bid, 4)
        self.assertEqual(yes_ask, 3)  # 100 - 97

    def test_legacy_empty(self):
        book = {"orderbook": {"yes": [], "no": []}}
        self.assertEqual(parse_book_top(book), (None, None))

    def test_fp_takes_precedence_over_legacy(self):
        """If both fields present, fp wins (it's the current schema)."""
        book = {
            "orderbook_fp": {
                "yes_dollars": [["0.9000", "1.00"]],
                "no_dollars": [["0.0500", "1.00"]],
            },
            "orderbook": {
                "yes": [[10, 1]],
                "no": [[20, 1]],
            },
        }
        yes_bid, yes_ask = parse_book_top(book)
        self.assertEqual(yes_bid, 90, "should use orderbook_fp, not legacy")
        self.assertEqual(yes_ask, 95)


class TestRegressionScenarios(unittest.TestCase):
    """Regression tests for the specific 2026-04-13 bug scenarios."""

    def test_wrong_pre_fix_behavior_not_reproducible(self):
        """The pre-fix code read book.get('orderbook', {}).get('yes', []) which
        returned [] under the fp schema, making yes_ask None → fallback to max_price.
        Ensure parse_book_top actually returns meaningful values on fp data."""
        book = {
            "orderbook_fp": {
                "yes_dollars": [["0.0200", "100"]],
                "no_dollars": [["0.9500", "100"]],
            }
        }
        yes_bid, yes_ask = parse_book_top(book)
        self.assertEqual(yes_bid, 2)
        self.assertEqual(yes_ask, 5)
        # Pre-fix would have seen yes_ask=None and fallen back to RR's max_price (98c).


if __name__ == "__main__":
    unittest.main()
