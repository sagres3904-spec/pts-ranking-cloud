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
    return namespace["safe_attach_disclosures"], namespace["requests"], namespace["st"]


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class CodeSpecificDisclosureBackfillTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        safe_attach_disclosures, requests_module, st_module = load_disclosure_helpers()
        cls.safe_attach_disclosures = staticmethod(safe_attach_disclosures)
        cls.requests_module = requests_module
        cls.st_module = st_module

    def run_attach(self, df_in, today_items, yesterday_items, code_items_by_url=None, debug=False):
        if code_items_by_url is None:
            code_items_by_url = {}

        requested_urls = []

        def fake_get(url, timeout=20):
            requested_urls.append(url)
            if "today.json2" in url:
                return FakeResponse({"items": today_items})
            if "yesterday.json2" in url:
                return FakeResponse({"items": yesterday_items})
            for marker, items in code_items_by_url.items():
                if marker in url:
                    return FakeResponse({"items": items})
            return FakeResponse({"items": []})

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            if debug:
                with patch.object(self.st_module, "write"), patch.object(self.st_module, "dataframe"):
                    out, err = self.safe_attach_disclosures(df_in, debug=True)
            else:
                out, err = self.safe_attach_disclosures(df_in, debug=False)

        return out, err, requested_urls

    def signpost_rows(self):
        return pd.DataFrame([{"code": "3996", "name": "サインポスト", "pct": 12.3, "volume": 399600}])

    def signpost_global_items(self):
        return [
            {
                "company_code": "3996",
                "title": "業績予想の修正及び配当予想の修正に関するお知らせ",
                "document_url": "https://example.com/docs/3996-forecast.pdf?from=today",
                "pubdate": "2026-07-10 15:30",
            }
        ]

    def signpost_code_items(self):
        return [
            {
                "company_code": "3996",
                "title": "業績予想の修正及び配当予想の修正に関するお知らせ",
                "document_url": "https://example.com/docs/3996-forecast.pdf?from=code",
                "pubdate": "2026-07-10 15:30",
            },
            {
                "company_code": "3996",
                "title": "2027年2月期 第1四半期決算短信〔日本基準〕(非連結)",
                "document_url": "https://example.com/docs/3996-earnings.pdf",
                "pubdate": "2026-07-10 15:30",
            },
        ]

    def test_signpost_code_specific_backfill_adds_second_disclosure(self):
        out, err, _urls = self.run_attach(
            self.signpost_rows(),
            self.signpost_global_items(),
            [],
            {"list/3996.json2": self.signpost_code_items()},
        )

        self.assertIsNone(err)
        self.assertEqual(int(out.loc[0, "開示件数"]), 2)

    def test_signpost_pdf_link_1_and_2_are_distinct_urls(self):
        out, _err, _urls = self.run_attach(
            self.signpost_rows(),
            self.signpost_global_items(),
            [],
            {"list/3996.json2": self.signpost_code_items()},
        )

        pdf_links = [out.loc[0, "PDFリンク1"], out.loc[0, "PDFリンク2"]]
        self.assertEqual(len(set(pdf_links)), 2)
        self.assertIn("https://example.com/docs/3996-forecast.pdf?from=today", pdf_links)
        self.assertIn("https://example.com/docs/3996-earnings.pdf", pdf_links)

    def test_signpost_title_1_and_2_are_both_present(self):
        out, _err, _urls = self.run_attach(
            self.signpost_rows(),
            self.signpost_global_items(),
            [],
            {"list/3996.json2": self.signpost_code_items()},
        )

        titles = out.loc[0, "_開示上位5"]
        title_text = "\n".join(item["title"] for item in titles)
        self.assertIn("業績予想の修正及び配当予想の修正に関するお知らせ", title_text)
        self.assertIn("2027年2月期 第1四半期決算短信", title_text)

    def test_same_pdf_from_global_and_code_specific_is_deduped_by_code_and_doc_identity(self):
        out, _err, _urls = self.run_attach(
            self.signpost_rows(),
            self.signpost_global_items(),
            [],
            {"list/3996.json2": [self.signpost_code_items()[0]]},
        )

        self.assertEqual(int(out.loc[0, "開示件数"]), 1)
        self.assertEqual(out.loc[0, "PDFリンク2"], "")

    def test_same_code_with_different_document_url_stays_multiple(self):
        out, _err, _urls = self.run_attach(
            self.signpost_rows(),
            [],
            [],
            {"list/3996.json2": self.signpost_code_items()},
        )

        self.assertEqual(int(out.loc[0, "開示件数"]), 2)
        self.assertNotEqual(out.loc[0, "PDFリンク1"], out.loc[0, "PDFリンク2"])

    def test_code_specific_timeout_keeps_global_success_without_error(self):
        df_in = self.signpost_rows()

        def fake_get(url, timeout=20):
            if "today.json2" in url:
                return FakeResponse({"items": self.signpost_global_items()})
            if "yesterday.json2" in url:
                return FakeResponse({"items": []})
            if "list/3996.json2" in url:
                raise self.requests_module.exceptions.ReadTimeout("read timed out")
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach_disclosures(df_in, debug=False)

        self.assertIsNone(err)
        self.assertEqual(int(out.loc[0, "開示件数"]), 1)
        self.assertEqual(out.loc[0, "PDFリンク1"], "https://example.com/docs/3996-forecast.pdf?from=today")
        info = out.attrs["yanoshin_debug_info"]
        self.assertEqual(info["code_specific_failed_count"], 1)

    def test_alphanumeric_codes_are_kept_in_code_specific_url(self):
        df_in = pd.DataFrame(
            [
                {"code": "446A", "name": "英字候補A"},
                {"code": "130A", "name": "英字候補B"},
            ]
        )
        out, err, urls = self.run_attach(df_in, [], [], {"list/446A-130A.json2": []})

        self.assertEqual(err, "no disclosure data")
        self.assertEqual(int(out["開示件数"].sum()), 0)
        code_urls = [url for url in urls if "tdnet/list/" in url and "today.json2" not in url and "yesterday.json2" not in url]
        self.assertEqual(len(code_urls), 1)
        self.assertIn("list/446A-130A.json2?limit=300", code_urls[0])

    def test_debug_true_and_false_keep_same_disclosure_outputs(self):
        df_in = self.signpost_rows()
        code_items = {"list/3996.json2": self.signpost_code_items()}

        out_debug_off, err_debug_off, _ = self.run_attach(
            df_in,
            self.signpost_global_items(),
            [],
            code_items,
            debug=False,
        )
        out_debug_on, err_debug_on, _ = self.run_attach(
            df_in,
            self.signpost_global_items(),
            [],
            code_items,
            debug=True,
        )

        self.assertIsNone(err_debug_off)
        self.assertIsNone(err_debug_on)
        for col in ["開示件数", "PDFリンク1", "PDFリンク2", "PDFリンク3", "_開示上位5"]:
            self.assertEqual(out_debug_off.loc[0, col], out_debug_on.loc[0, col], col)


if __name__ == "__main__":
    unittest.main()
