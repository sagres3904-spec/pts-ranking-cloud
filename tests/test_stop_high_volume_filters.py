import pathlib
import unittest

import pandas as pd


def load_filter_candidate_stocks():
    app_path = pathlib.Path(__file__).resolve().parents[1] / "app.py"
    source = app_path.read_text(encoding="utf-8")
    source = source.split("# ========= UI =========", 1)[0]
    namespace = {}
    exec(compile(source, str(app_path), "exec"), namespace)
    return namespace["filter_candidate_stocks"]


class StopHighVolumeFilterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.filter_candidate_stocks = staticmethod(load_filter_candidate_stocks())

    def test_case_a_stop_high_survives_when_ignore_is_on(self):
        df = pd.DataFrame(
            [
                {"code": "1111", "pct": 10.0, "volume": 100, "is_stop_high": True},
            ]
        )

        out = self.filter_candidate_stocks(
            df=df,
            pct_min=5.0,
            vol_min=1000,
            ignore_volume_for_stop_high=True,
        )

        self.assertEqual(list(out["code"]), ["1111"])

    def test_case_b_non_stop_high_still_fails_when_ignore_is_on(self):
        df = pd.DataFrame(
            [
                {"code": "2222", "pct": 10.0, "volume": 100, "is_stop_high": False},
            ]
        )

        out = self.filter_candidate_stocks(
            df=df,
            pct_min=5.0,
            vol_min=1000,
            ignore_volume_for_stop_high=True,
        )

        self.assertTrue(out.empty)

    def test_case_c_stop_high_fails_when_ignore_is_off(self):
        df = pd.DataFrame(
            [
                {"code": "3333", "pct": 10.0, "volume": 100, "is_stop_high": True},
            ]
        )

        out = self.filter_candidate_stocks(
            df=df,
            pct_min=5.0,
            vol_min=1000,
            ignore_volume_for_stop_high=False,
        )

        self.assertTrue(out.empty)

    def test_case_d_stop_high_with_missing_volume_survives_when_ignore_is_on(self):
        df = pd.DataFrame(
            [
                {"code": "4444", "pct": 10.0, "volume": None, "is_stop_high": True},
            ]
        )

        out = self.filter_candidate_stocks(
            df=df,
            pct_min=5.0,
            vol_min=1000,
            ignore_volume_for_stop_high=True,
        )

        self.assertEqual(list(out["code"]), ["4444"])


if __name__ == "__main__":
    unittest.main()
