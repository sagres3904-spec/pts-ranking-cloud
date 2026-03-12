import pathlib
import types
import unittest
from unittest.mock import patch

import pandas as pd


def load_attach_disclosures():
    app_path = pathlib.Path(__file__).resolve().parents[1] / "app.py"
    source = app_path.read_text(encoding="utf-8")
    source = source.split("# ========= UI =========", 1)[0]
    namespace = {}
    exec(compile(source, str(app_path), "exec"), namespace)
    return namespace["attach_disclosures"], namespace["requests"]


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class AttachDisclosuresDedupeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        attach_disclosures, requests_module = load_attach_disclosures()
        cls.attach_disclosures = staticmethod(attach_disclosures)
        cls.requests_module = requests_module

    def run_attach(self, today_items, yesterday_items, input_codes):
        payloads = [
            {"items": today_items},
            {"items": yesterday_items},
        ]

        def fake_get(url, timeout=20):
            if "today.json2" in url:
                return FakeResponse(payloads[0])
            if "yesterday.json2" in url:
                return FakeResponse(payloads[1])
            raise AssertionError(f"unexpected url: {url}")

        df_in = pd.DataFrame({"code": input_codes})
        with patch.object(self.requests_module, "get", side_effect=fake_get):
            return self.attach_disclosures(df_in, debug=False)

    def test_case_a_query_only_difference_is_deduped(self):
        today = [
            {
                "company_code": "1111",
                "title": "決算短信",
                "document_url": "https://example.com/docs/a.pdf?token=today",
                "pubdate": "2026-03-12 00:10",
            }
        ]
        yesterday = [
            {
                "company_code": "1111",
                "title": "決算短信",
                "document_url": "https://example.com/docs/a.pdf?token=yesterday",
                "pubdate": "2026-03-11 23:59",
            }
        ]

        out = self.run_attach(today, yesterday, ["1111"])
        self.assertEqual(int(out.loc[0, "開示件数"]), 1)

    def test_case_b_host_case_fragment_and_trailing_slash_are_deduped(self):
        today = [
            {
                "company_code": "1111",
                "title": "適時開示",
                "document_url": "HTTPS://EXAMPLE.COM/docs/b.pdf#top",
                "pubdate": "2026-03-12 00:20",
            }
        ]
        yesterday = [
            {
                "company_code": "1111",
                "title": "適時開示",
                "document_url": "https://example.com/docs/b.pdf/",
                "pubdate": "2026-03-11 23:58",
            }
        ]

        out = self.run_attach(today, yesterday, ["1111"])
        self.assertEqual(int(out.loc[0, "開示件数"]), 1)

    def test_case_c_same_filename_but_different_paths_stay_separate(self):
        today = [
            {
                "company_code": "1111",
                "title": "開示A",
                "document_url": "https://example.com/20260312/shared.pdf",
                "pubdate": "2026-03-12 00:30",
            }
        ]
        yesterday = [
            {
                "company_code": "1111",
                "title": "開示B",
                "document_url": "https://example.com/20260311/shared.pdf",
                "pubdate": "2026-03-11 23:57",
            }
        ]

        out = self.run_attach(today, yesterday, ["1111"])
        self.assertEqual(int(out.loc[0, "開示件数"]), 2)

    def test_case_d_similar_titles_but_different_paths_stay_separate(self):
        today = [
            {
                "company_code": "1111",
                "title": "自己株式の取得状況に関するお知らせ",
                "document_url": "https://example.com/docs/status_march.pdf",
                "pubdate": "2026-03-12 00:40",
            }
        ]
        yesterday = [
            {
                "company_code": "1111",
                "title": "自己株式の取得状況に関するお知らせ（訂正）",
                "document_url": "https://example.com/docs/status_march_fix.pdf",
                "pubdate": "2026-03-11 23:56",
            }
        ]

        out = self.run_attach(today, yesterday, ["1111"])
        self.assertEqual(int(out.loc[0, "開示件数"]), 2)

    def test_case_e_same_url_but_different_codes_stay_separate(self):
        shared_url = "https://example.com/docs/shared.pdf?session=abc"
        today = [
            {
                "company_code": "1111",
                "title": "銘柄1111の開示",
                "document_url": shared_url,
                "pubdate": "2026-03-12 00:50",
            }
        ]
        yesterday = [
            {
                "company_code": "2222",
                "title": "銘柄2222の開示",
                "document_url": shared_url,
                "pubdate": "2026-03-11 23:55",
            }
        ]

        out = self.run_attach(today, yesterday, ["1111", "2222"])
        counts = dict(zip(out["code"], out["開示件数"]))
        self.assertEqual(int(counts["1111"]), 1)
        self.assertEqual(int(counts["2222"]), 1)


if __name__ == "__main__":
    unittest.main()
