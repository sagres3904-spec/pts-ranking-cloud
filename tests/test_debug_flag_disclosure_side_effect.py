import pathlib
import unittest
from unittest.mock import patch

import pandas as pd


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def load_disclosure_helpers():
    app_path = pathlib.Path(__file__).resolve().parents[1] / "app.py"
    source = app_path.read_text(encoding="utf-8")
    source = source.split("# ========= UI =========", 1)[0]
    namespace = {}
    exec(compile(source, str(app_path), "exec"), namespace)
    return (
        namespace["safe_attach_disclosures"],
        namespace["_prepare_results_display_dataframe"],
        namespace["requests"],
        namespace["st"],
    )


class DebugFlagDisclosureSideEffectTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        safe_attach_disclosures, prepare_display_dataframe, requests_module, st_module = load_disclosure_helpers()
        cls.safe_attach_disclosures = staticmethod(safe_attach_disclosures)
        cls.prepare_display_dataframe = staticmethod(prepare_display_dataframe)
        cls.requests_module = requests_module
        cls.st_module = st_module

    def run_safe_attach(self, df_in, today_items, yesterday_items=None, debug=False):
        if yesterday_items is None:
            yesterday_items = []

        def fake_get(url, timeout=20):
            if "today.json2" in url:
                return FakeResponse({"items": today_items})
            if "yesterday.json2" in url:
                return FakeResponse({"items": yesterday_items})
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            if debug:
                with patch.object(self.st_module, "write"), patch.object(self.st_module, "dataframe"):
                    return self.safe_attach_disclosures(df_in, debug=True)
            return self.safe_attach_disclosures(df_in, debug=False)

    def assert_disclosure_columns_equal(self, left, right):
        columns = [
            "開示件数",
            "開示タイトル1",
            "開示タイトル2",
            "開示タイトル3",
            "PDFリンク1",
            "PDFリンク2",
            "PDFリンク3",
            "_開示上位5",
        ]
        for col in columns:
            self.assertEqual(left.loc[0, col], right.loc[0, col], col)

    def test_safe_attach_debug_flag_keeps_multiple_disclosure_columns_identical(self):
        df_in = pd.DataFrame([{"code": "3628", "name": "データホライゾン", "pct": 12.3, "volume": 12300}])
        today = [
            {
                "company_code": "3628",
                "title": "開示A",
                "document_url": "https://example.com/docs/3628-a.pdf",
                "pubdate": "2026-05-12 15:00",
            },
            {
                "company_code": "3628",
                "title": "開示B",
                "document_url": "https://example.com/docs/3628-b.pdf",
                "pubdate": "2026-05-12 15:01",
            },
            {
                "company_code": "3628",
                "title": "開示C",
                "document_url": "https://example.com/docs/3628-c.pdf",
                "pubdate": "2026-05-12 15:02",
            },
        ]

        out_debug_off, err_debug_off = self.run_safe_attach(df_in, today, debug=False)
        out_debug_on, err_debug_on = self.run_safe_attach(df_in, today, debug=True)

        self.assertIsNone(err_debug_off)
        self.assertIsNone(err_debug_on)
        self.assertEqual(len(out_debug_off), len(out_debug_on))
        self.assert_disclosure_columns_equal(out_debug_off, out_debug_on)

    def test_3628_multiple_disclosures_survive_with_debug_false(self):
        df_in = pd.DataFrame([{"code": "3628", "name": "データホライゾン", "pct": 12.3, "volume": 12300}])
        today = [
            {
                "company_code": "3628",
                "title": "開示A",
                "document_url": "https://example.com/docs/3628-a.pdf",
                "pubdate": "2026-05-12 15:00",
            },
            {
                "company_code": "3628",
                "title": "開示B",
                "document_url": "https://example.com/docs/3628-b.pdf",
                "pubdate": "2026-05-12 15:01",
            },
        ]

        out, err = self.run_safe_attach(df_in, today, debug=False)

        self.assertIsNone(err)
        self.assertGreaterEqual(int(out.loc[0, "開示件数"]), 2)
        self.assertEqual(out.loc[0, "PDFリンク2"], "https://example.com/docs/3628-b.pdf")

    def test_debug_true_only_disclosure_regression_does_not_recur(self):
        df_in = pd.DataFrame([{"code": "3628", "name": "データホライゾン", "pct": 12.3, "volume": 12300}])
        today = [
            {
                "company_code": "3628",
                "title": "開示A",
                "document_url": "https://example.com/docs/3628-a.pdf",
                "pubdate": "2026-05-12 15:00",
            }
        ]

        out_debug_off, _ = self.run_safe_attach(df_in, today, debug=False)
        out_debug_on, _ = self.run_safe_attach(df_in, today, debug=True)

        self.assertEqual(int(out_debug_off.loc[0, "開示件数"]), 1)
        self.assertEqual(int(out_debug_off.loc[0, "開示件数"]), int(out_debug_on.loc[0, "開示件数"]))
        self.assertEqual(out_debug_off.loc[0, "PDFリンク1"], out_debug_on.loc[0, "PDFリンク1"])

    def test_display_dataframe_keeps_pdf_links_from_debug_on_and_off_results(self):
        df_in = pd.DataFrame([{"code": "3628", "name": "データホライゾン", "pct": 12.3, "volume": 12300}])
        today = [
            {
                "company_code": "3628",
                "title": "開示A",
                "document_url": "https://example.com/docs/3628-a.pdf",
                "pubdate": "2026-05-12 15:00",
            },
            {
                "company_code": "3628",
                "title": "開示B",
                "document_url": "https://example.com/docs/3628-b.pdf",
                "pubdate": "2026-05-12 15:01",
            },
            {
                "company_code": "3628",
                "title": "開示C",
                "document_url": "https://example.com/docs/3628-c.pdf",
                "pubdate": "2026-05-12 15:02",
            },
        ]
        out_debug_off, _ = self.run_safe_attach(df_in, today, debug=False)
        out_debug_on, _ = self.run_safe_attach(df_in, today, debug=True)

        display_off = self.prepare_display_dataframe(out_debug_off)
        display_on = self.prepare_display_dataframe(out_debug_on)

        for display_df in [display_off, display_on]:
            self.assertEqual(display_df.loc[0, "PDFリンク1"], "https://example.com/docs/3628-a.pdf")
            self.assertEqual(display_df.loc[0, "PDFリンク2"], "https://example.com/docs/3628-b.pdf")
            self.assertEqual(display_df.loc[0, "PDFリンク3"], "https://example.com/docs/3628-c.pdf")

    def test_all_timeout_keeps_pts_rows_and_empty_disclosures_for_both_debug_modes(self):
        df_in = pd.DataFrame(
            [
                {"code": "1111", "name": "候補A", "pct": 10.0, "volume": 1000},
                {"code": "2222", "name": "候補B", "pct": 9.0, "volume": 2000},
            ]
        )

        def raise_timeout(*_args, **_kwargs):
            raise self.requests_module.exceptions.ReadTimeout("read timed out")

        with patch.object(self.requests_module, "get", side_effect=raise_timeout):
            out_debug_off, err_debug_off = self.safe_attach_disclosures(df_in, debug=False)

        with patch.object(self.requests_module, "get", side_effect=raise_timeout):
            with patch.object(self.st_module, "write"), patch.object(self.st_module, "dataframe"):
                out_debug_on, err_debug_on = self.safe_attach_disclosures(df_in, debug=True)

        self.assertEqual(err_debug_off, "timeout")
        self.assertEqual(err_debug_on, "timeout")
        self.assertEqual(list(out_debug_off["code"]), list(out_debug_on["code"]))
        self.assertEqual(int(out_debug_off["開示件数"].sum()), 0)
        self.assertEqual(int(out_debug_on["開示件数"].sum()), 0)
        self.assertEqual(out_debug_off.loc[0, "PDFリンク1"], "")
        self.assertEqual(out_debug_on.loc[0, "PDFリンク1"], "")

    def test_partial_success_keeps_successful_links_for_both_debug_modes(self):
        df_in = pd.DataFrame([{"code": "1111", "name": "候補A", "pct": 10.0, "volume": 1000}])
        yesterday = [
            {
                "company_code": "1111",
                "title": "成功した開示",
                "document_url": "https://example.com/docs/success.pdf",
                "pubdate": "2026-05-10 15:00",
            }
        ]

        def fake_get(url, timeout=20):
            if "today.json2" in url:
                raise self.requests_module.exceptions.ReadTimeout("read timed out")
            if "yesterday.json2" in url:
                return FakeResponse({"items": yesterday})
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out_debug_off, err_debug_off = self.safe_attach_disclosures(df_in, debug=False)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            with patch.object(self.st_module, "write"), patch.object(self.st_module, "dataframe"):
                out_debug_on, err_debug_on = self.safe_attach_disclosures(df_in, debug=True)

        self.assertEqual(err_debug_off, "partial: timeout")
        self.assertEqual(err_debug_on, "partial: timeout")
        self.assertEqual(int(out_debug_off.loc[0, "開示件数"]), 1)
        self.assertEqual(int(out_debug_on.loc[0, "開示件数"]), 1)
        self.assertEqual(out_debug_off.loc[0, "PDFリンク1"], "https://example.com/docs/success.pdf")
        self.assertEqual(out_debug_off.loc[0, "PDFリンク1"], out_debug_on.loc[0, "PDFリンク1"])


if __name__ == "__main__":
    unittest.main()
