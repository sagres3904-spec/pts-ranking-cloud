import pathlib
import unittest
from unittest.mock import patch

import pandas as pd


def load_disclosure_helpers():
    app_path = pathlib.Path(__file__).resolve().parents[1] / "app.py"
    source = app_path.read_text(encoding="utf-8")
    source = source.split("# ========= UI =========", 1)[0]
    namespace = {}
    exec(compile(source, str(app_path), "exec"), namespace)
    return (
        namespace["attach_disclosures"],
        namespace["_prepare_results_display_dataframe"],
        namespace["requests"],
        namespace["st"],
    )


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class MultipleDisclosuresPerCompanyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        attach_disclosures, prepare_display_dataframe, requests_module, st_module = load_disclosure_helpers()
        cls.attach_disclosures = staticmethod(attach_disclosures)
        cls.prepare_display_dataframe = staticmethod(prepare_display_dataframe)
        cls.requests_module = requests_module
        cls.st_module = st_module

    def run_attach(self, today_items, yesterday_items, input_rows=None, debug=False):
        if input_rows is None:
            input_rows = [{"code": "3628", "name": "データホライゾン", "pct": 12.3, "volume": 12300}]

        def fake_get(url, timeout=20):
            if "today.json2" in url:
                return FakeResponse({"items": today_items})
            if "yesterday.json2" in url:
                return FakeResponse({"items": yesterday_items})
            raise AssertionError(f"unexpected url: {url}")

        df_in = pd.DataFrame(input_rows)
        with patch.object(self.requests_module, "get", side_effect=fake_get):
            if debug:
                with patch.object(self.st_module, "write"), patch.object(self.st_module, "dataframe"):
                    return self.attach_disclosures(df_in, debug=True)
            return self.attach_disclosures(df_in, debug=False)

    def test_case_a_3628_keeps_three_distinct_document_urls(self):
        today = [
            {
                "company_code": "3628",
                "title": "データホライゾン 開示A",
                "document_url": "https://example.com/docs/3628-a.pdf",
                "pubdate": "2026-05-12 15:00",
            },
            {
                "company_code": "3628",
                "title": "データホライゾン 開示B",
                "document_url": "https://example.com/docs/3628-b.pdf",
                "pubdate": "2026-05-12 15:01",
            },
            {
                "company_code": "3628",
                "title": "データホライゾン 開示C",
                "document_url": "https://example.com/docs/3628-c.pdf",
                "pubdate": "2026-05-12 15:02",
            },
        ]

        out = self.run_attach(today, [])

        self.assertEqual(int(out.loc[0, "開示件数"]), 3)
        self.assertEqual(out.loc[0, "PDFリンク1"], "https://example.com/docs/3628-a.pdf")
        self.assertEqual(out.loc[0, "PDFリンク2"], "https://example.com/docs/3628-b.pdf")
        self.assertEqual(out.loc[0, "PDFリンク3"], "https://example.com/docs/3628-c.pdf")
        self.assertIn("開示A", out.loc[0, "開示タイトル1"])
        self.assertIn("開示B", out.loc[0, "開示タイトル2"])
        self.assertIn("開示C", out.loc[0, "開示タイトル3"])
        self.assertEqual(len(out.loc[0, "_開示上位5"]), 3)

    def test_case_b_same_document_url_is_deduped_to_one(self):
        today = [
            {
                "company_code": "3628",
                "title": "重複開示",
                "document_url": "https://example.com/docs/3628-dup.pdf?token=a",
                "pubdate": "2026-05-12 15:00",
            },
            {
                "company_code": "3628",
                "title": "重複開示",
                "document_url": "https://example.com/docs/3628-dup.pdf?token=b",
                "pubdate": "2026-05-12 15:01",
            },
        ]

        out = self.run_attach(today, [])

        self.assertEqual(int(out.loc[0, "開示件数"]), 1)
        self.assertEqual(out.loc[0, "PDFリンク1"], "https://example.com/docs/3628-dup.pdf?token=a")
        self.assertEqual(out.loc[0, "PDFリンク2"], "")

    def test_case_c_similar_titles_with_different_document_urls_stay_separate(self):
        today = [
            {
                "company_code": "3628",
                "title": "業績予想の修正に関するお知らせ",
                "document_url": "https://example.com/docs/3628-forecast.pdf",
                "pubdate": "2026-05-12 15:00",
            },
            {
                "company_code": "3628",
                "title": "業績予想の修正に関するお知らせ（訂正）",
                "document_url": "https://example.com/docs/3628-forecast-fix.pdf",
                "pubdate": "2026-05-12 15:01",
            },
        ]

        out = self.run_attach(today, [])

        self.assertEqual(int(out.loc[0, "開示件数"]), 2)
        self.assertEqual(out.loc[0, "PDFリンク1"], "https://example.com/docs/3628-forecast.pdf")
        self.assertEqual(out.loc[0, "PDFリンク2"], "https://example.com/docs/3628-forecast-fix.pdf")

    def test_case_d_display_dataframe_keeps_pdf_link_2_and_3(self):
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

        out = self.run_attach(today, [])
        display_df = self.prepare_display_dataframe(out)

        self.assertIn("PDFリンク2", display_df.columns)
        self.assertIn("PDFリンク3", display_df.columns)
        self.assertEqual(display_df.loc[0, "PDFリンク2"], "https://example.com/docs/3628-b.pdf")
        self.assertEqual(display_df.loc[0, "PDFリンク3"], "https://example.com/docs/3628-c.pdf")

    def test_case_e_debug_flag_does_not_change_disclosure_count(self):
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

        out_debug_off = self.run_attach(today, [], debug=False)
        out_debug_on = self.run_attach(today, [], debug=True)

        self.assertEqual(int(out_debug_off.loc[0, "開示件数"]), 2)
        self.assertEqual(int(out_debug_on.loc[0, "開示件数"]), 2)
        self.assertEqual(out_debug_off.loc[0, "PDFリンク2"], out_debug_on.loc[0, "PDFリンク2"])

    def test_case_f_alphanumeric_code_446a_keeps_multiple_disclosures(self):
        today = [
            {
                "company_code": "446A0",
                "title": "英字コード開示A",
                "document_url": "https://example.com/docs/446a-a.pdf",
                "pubdate": "2026-05-12 15:00",
            },
            {
                "company_code": "446a0",
                "title": "英字コード開示B",
                "document_url": "https://example.com/docs/446a-b.pdf",
                "pubdate": "2026-05-12 15:01",
            },
        ]
        rows = [{"code": "446A", "name": "英字コード銘柄", "pct": 9.5, "volume": 44600}]

        out = self.run_attach(today, [], input_rows=rows)

        self.assertEqual(out.loc[0, "code"], "446A")
        self.assertEqual(int(out.loc[0, "開示件数"]), 2)
        self.assertEqual(out.loc[0, "PDFリンク1"], "https://example.com/docs/446a-a.pdf")
        self.assertEqual(out.loc[0, "PDFリンク2"], "https://example.com/docs/446a-b.pdf")


if __name__ == "__main__":
    unittest.main()
