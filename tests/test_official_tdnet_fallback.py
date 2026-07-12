import datetime
import pathlib
import unittest
from unittest.mock import patch

import pandas as pd


ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"


def load_disclosure_helpers():
    app_path = ROOT / "app.py"
    source = app_path.read_text(encoding="utf-8")
    source = source.split("# ========= UI =========", 1)[0]
    namespace = {}
    exec(compile(source, str(app_path), "exec"), namespace)
    return namespace


class FakeResponse:
    def __init__(self, payload=None, text="", status_code=200, json_exc=None):
        self._payload = payload
        self.text = text
        self.status_code = status_code
        self.encoding = "utf-8"
        self.apparent_encoding = "utf-8"
        self._json_exc = json_exc

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            err = requests.exceptions.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err
        return None

    def json(self):
        if self._json_exc is not None:
            raise self._json_exc
        return self._payload


class OfficialTdnetFallbackTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ns = load_disclosure_helpers()
        cls.safe_attach = staticmethod(cls.ns["safe_attach_disclosures"])
        cls.parse_page = staticmethod(cls.ns["_parse_tdnet_official_page"])
        cls.fetch_official_uncached = staticmethod(cls.ns["_fetch_tdnet_official_for_dates_uncached"])
        cls.requests_module = cls.ns["requests"]
        cls.st_module = cls.ns["st"]
        cls.requirements_before = (ROOT / "requirements.txt").read_text(encoding="utf-8")

    def setUp(self):
        cached = self.ns.get("_fetch_tdnet_official_for_dates_cached")
        if cached is not None and hasattr(cached, "clear"):
            cached.clear()

    def fixture(self, name):
        return (FIXTURES / name).read_text(encoding="utf-8")

    def df_candidates(self, codes):
        return pd.DataFrame([{"code": code, "name": f"銘柄{code}", "pct": 10.0, "volume": 1000} for code in codes])

    def yanoshin_item(self, code, url, title="Yanoshin開示", pubdate="2026-07-10 15:00"):
        return {"company_code": code, "title": title, "document_url": url, "pubdate": pubdate}

    def official_response(self, url):
        if url.endswith("I_main_00.html"):
            return FakeResponse(text=self.fixture("tdnet_official_main.html"))
        if url.endswith("I_list_001_20260710.html"):
            return FakeResponse(text=self.fixture("tdnet_official_list_page_1.html"))
        if url.endswith("I_list_002_20260710.html"):
            return FakeResponse(text=self.fixture("tdnet_official_list_page_2.html"))
        if url.endswith("I_list_001_20260709.html"):
            return FakeResponse(text=self.fixture("tdnet_official_empty.html"))
        if url.endswith("I_list_001_20260713.html"):
            return FakeResponse(text=self.fixture("tdnet_official_list_page_1.html").replace("20260710", "20260713").replace("2026年07月10日", "2026年07月13日"))
        if url.endswith("I_list_002_20260713.html"):
            return FakeResponse(text=self.fixture("tdnet_official_list_page_2.html").replace("20260710", "20260713").replace("2026年07月10日", "2026年07月13日"))
        if url.endswith("I_list_001_20260712.html"):
            return FakeResponse(text=self.fixture("tdnet_official_empty.html"))
        raise AssertionError(f"unexpected official url: {url}")

    def test_01_yanoshin_success_does_not_call_official_tdnet(self):
        requested = []

        def fake_get(url, **_kwargs):
            requested.append(url)
            if "today.json2" in url:
                return FakeResponse({"items": [self.yanoshin_item("7523", "https://example.com/y.pdf")]})
            if "yesterday.json2" in url or "tdnet/list/7523" in url:
                return FakeResponse({"items": []})
            raise AssertionError(f"official should not be called: {url}")

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach(self.df_candidates(["7523"]), debug=False)

        self.assertIsNone(err)
        self.assertEqual(int(out.loc[0, "開示件数"]), 1)
        self.assertFalse(any("release.tdnet.info" in url for url in requested))

    def test_02_both_yanoshin_timeouts_attach_official_tdnet_success(self):
        def fake_get(url, **_kwargs):
            if "webapi.yanoshin.jp" in url:
                raise self.requests_module.exceptions.ReadTimeout("read timed out")
            return self.official_response(url)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach(self.df_candidates(["7523"]), debug=False)

        self.assertEqual(err, "tdnet_official_all")
        self.assertEqual(int(out.loc[0, "開示件数"]), 3)
        self.assertIn("591505.pdf", out.loc[0, "PDFリンク1"])

    def test_03_invalid_yanoshin_json_falls_back_to_official(self):
        def fake_get(url, **_kwargs):
            if "webapi.yanoshin.jp" in url:
                return FakeResponse(json_exc=ValueError("bad json"))
            return self.official_response(url)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach(self.df_candidates(["446A"]), debug=False)

        self.assertEqual(err, "tdnet_official_all")
        self.assertEqual(out.loc[0, "code"], "446A")
        self.assertEqual(int(out.loc[0, "開示件数"]), 1)

    def test_04_yanoshin_zero_valid_rows_falls_back_to_official(self):
        def fake_get(url, **_kwargs):
            if "webapi.yanoshin.jp" in url:
                return FakeResponse({"items": [{"company_code": "7523", "title": "URLなし", "document_url": "", "pubdate": "2026-07-10 15:00"}]})
            return self.official_response(url)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach(self.df_candidates(["7523"]), debug=False)

        self.assertEqual(err, "tdnet_official_all")
        self.assertEqual(int(out.loc[0, "開示件数"]), 3)

    def test_05_today_success_yesterday_failure_merges_yanoshin_and_official(self):
        def fake_get(url, **_kwargs):
            if "today.json2" in url:
                return FakeResponse({"items": [self.yanoshin_item("7523", "https://example.com/yanoshin-only.pdf")]})
            if "yesterday.json2" in url:
                raise self.requests_module.exceptions.ReadTimeout("read timed out")
            if "webapi.yanoshin.jp" in url:
                return FakeResponse({"items": []})
            return self.official_response(url)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach(self.df_candidates(["7523"]), debug=False)

        self.assertEqual(err, "tdnet_official_partial")
        links = [out.loc[0, f"PDFリンク{i}"] for i in [1, 2, 3]]
        self.assertIn("https://example.com/yanoshin-only.pdf", links)
        self.assertTrue(any("140120260710591505.pdf" in link for link in links))

    def test_06_code_backfill_failure_uses_official_tdnet_for_target_dates(self):
        def fake_get(url, **_kwargs):
            if "today.json2" in url:
                return FakeResponse({"items": [self.yanoshin_item("1111", "https://example.com/base.pdf")]})
            if "yesterday.json2" in url:
                return FakeResponse({"items": []})
            if "webapi.yanoshin.jp" in url:
                raise self.requests_module.exceptions.ConnectionError("code backfill down")
            return self.official_response(url)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach(self.df_candidates(["3996"]), debug=False)

        self.assertEqual(err, "tdnet_official_partial")
        self.assertEqual(int(out.loc[0, "開示件数"]), 1)
        self.assertIn("399601.pdf", out.loc[0, "PDFリンク1"])

    def test_07_parse_company_code_name_title_time_and_pdf_url(self):
        page = self.fixture("tdnet_official_list_page_1.html")
        df, diag = self.parse_page(page, datetime.date(2026, 7, 10), "https://www.release.tdnet.info/inbs/I_list_001_20260710.html")

        first = df.iloc[0].to_dict()
        self.assertEqual(first["code"], "7523")
        self.assertEqual(first["title"], "公開買付けの開始に関するお知らせ")
        self.assertEqual(first["pubdate"], "2026-07-10 17:40:00")
        self.assertEqual(first["document_url"], "https://www.release.tdnet.info/inbs/140120260710591505.pdf")
        self.assertGreaterEqual(diag["dropped_rows"], 1)

    def test_08_relative_pdf_url_is_absolutized(self):
        page = self.fixture("tdnet_official_list_page_1.html")
        df, _diag = self.parse_page(page, datetime.date(2026, 7, 10), "https://www.release.tdnet.info/inbs/I_list_001_20260710.html")

        row = df[df["code"] == "446A"].iloc[0]
        self.assertEqual(row["document_url"], "https://www.release.tdnet.info/inbs/140120260710446001.pdf")

    def test_09_fetch_official_pager_collects_second_page(self):
        requested = []

        def fake_get(url, **_kwargs):
            requested.append(url)
            return self.official_response(url)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            df, diag = self.fetch_official_uncached({datetime.date(2026, 7, 10)}, max_pages=5)

        self.assertTrue(any(url.endswith("I_list_002_20260710.html") for url in requested))
        self.assertEqual(len(df[df["code"] == "7523"]), 3)
        self.assertEqual(diag["normalized_count"], 6)

    def test_10_same_company_three_different_pdfs_remain(self):
        def fake_get(url, **_kwargs):
            if "webapi.yanoshin.jp" in url:
                raise self.requests_module.exceptions.ReadTimeout("read timed out")
            return self.official_response(url)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, _err = self.safe_attach(self.df_candidates(["7523"]), debug=False)

        self.assertEqual(int(out.loc[0, "開示件数"]), 3)
        self.assertEqual(len({out.loc[0, f"PDFリンク{i}"] for i in [1, 2, 3]}), 3)

    def test_11_same_pdf_from_yanoshin_and_official_keeps_yanoshin_first(self):
        same_url = "https://www.release.tdnet.info/inbs/140120260710591505.pdf"

        def fake_get(url, **_kwargs):
            if "today.json2" in url:
                return FakeResponse({"items": [self.yanoshin_item("7523", same_url, title="Yanoshin優先")]})
            if "yesterday.json2" in url:
                raise self.requests_module.exceptions.ReadTimeout("read timed out")
            if "webapi.yanoshin.jp" in url:
                return FakeResponse({"items": []})
            return self.official_response(url)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, _err = self.safe_attach(self.df_candidates(["7523"]), debug=False)

        self.assertEqual(int(out.loc[0, "開示件数"]), 3)
        self.assertIn("Yanoshin優先", out.loc[0, "開示タイトル1"])

    def test_12_alphanumeric_codes_are_kept(self):
        def fake_get(url, **_kwargs):
            if "webapi.yanoshin.jp" in url:
                raise self.requests_module.exceptions.ReadTimeout("read timed out")
            return self.official_response(url)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, _err = self.safe_attach(self.df_candidates(["446A0", "130A"]), debug=False)

        counts = dict(zip(out["code"], out["開示件数"]))
        self.assertEqual(int(counts["446A"]), 1)
        self.assertEqual(int(counts["130A"]), 1)

    def test_13_birdman_target_date_window_still_excludes_old_44(self):
        df_in = self.df_candidates(["7063"])
        today = [self.yanoshin_item("1111", "https://example.com/base.pdf")]
        code_items = [
            self.yanoshin_item("7063", f"https://example.com/7063-{i}.pdf", title=f"Birdman {i}", pubdate=f"2026-07-{10 if i < 2 else 9:02d} 15:00")
            for i in range(3)
        ]
        code_items.extend(
            self.yanoshin_item("7063", f"https://example.com/old-{i}.pdf", title=f"old {i}", pubdate="2026-07-07 15:00")
            for i in range(44)
        )

        def fake_get(url, **_kwargs):
            if "today.json2" in url:
                return FakeResponse({"items": today})
            if "yesterday.json2" in url:
                return FakeResponse({"items": []})
            if "tdnet/list/7063" in url:
                return FakeResponse({"items": code_items})
            return FakeResponse({"items": []})

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach(df_in, debug=False)

        self.assertIsNone(err)
        self.assertEqual(int(out.loc[0, "開示件数"]), 3)

    def test_14_signpost_multiple_backfill_survives(self):
        df_in = self.df_candidates(["3996"])
        today = [self.yanoshin_item("3996", "https://example.com/3996-forecast.pdf", title="業績予想")]
        code_items = [
            self.yanoshin_item("3996", "https://example.com/3996-forecast.pdf?from=code", title="業績予想"),
            self.yanoshin_item("3996", "https://example.com/3996-earnings.pdf", title="決算短信"),
        ]

        def fake_get(url, **_kwargs):
            if "today.json2" in url:
                return FakeResponse({"items": today})
            if "yesterday.json2" in url:
                return FakeResponse({"items": []})
            if "tdnet/list/3996" in url:
                return FakeResponse({"items": code_items})
            return FakeResponse({"items": []})

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach(df_in, debug=False)

        self.assertIsNone(err)
        self.assertEqual(int(out.loc[0, "開示件数"]), 2)

    def test_15_official_tdnet_failure_keeps_pts_rows_empty_disclosures(self):
        def fake_get(url, **_kwargs):
            raise self.requests_module.exceptions.ReadTimeout("read timed out")

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach(self.df_candidates(["7523", "446A"]), debug=False)

        self.assertEqual(err, "timeout")
        self.assertEqual(len(out), 2)
        self.assertEqual(int(out["開示件数"].sum()), 0)

    def test_16_debug_true_false_results_match(self):
        def fake_get(url, **_kwargs):
            if "webapi.yanoshin.jp" in url:
                raise self.requests_module.exceptions.ReadTimeout("read timed out")
            return self.official_response(url)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            off, off_err = self.safe_attach(self.df_candidates(["7523", "446A"]), debug=False)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            with patch.object(self.st_module, "write"), patch.object(self.st_module, "dataframe"):
                on, on_err = self.safe_attach(self.df_candidates(["7523", "446A"]), debug=True)

        self.assertEqual(off_err, on_err)
        for col in ["開示件数", "開示タイトル1", "PDFリンク1", "開示タイトル2", "PDFリンク2", "開示タイトル3", "PDFリンク3", "_開示上位5"]:
            self.assertEqual(off[col].tolist(), on[col].tolist(), col)

    def test_17_and_18_requirements_are_unchanged_and_no_mcp_deps_added(self):
        after = (ROOT / "requirements.txt").read_text(encoding="utf-8")
        self.assertEqual(after, self.requirements_before)
        banned = ["tdnet-disclosure-mcp", "mcp", "httpx", "pydantic", "loguru"]
        lowered = after.lower()
        for dep in banned:
            self.assertNotIn(dep, lowered)


if __name__ == "__main__":
    unittest.main()
