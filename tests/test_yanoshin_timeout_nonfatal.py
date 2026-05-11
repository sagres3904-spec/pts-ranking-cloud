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


def load_yanoshin_helpers():
    app_path = pathlib.Path(__file__).resolve().parents[1] / "app.py"
    source = app_path.read_text(encoding="utf-8")
    source = source.split("# ========= UI =========", 1)[0]
    namespace = {}
    exec(compile(source, str(app_path), "exec"), namespace)
    return namespace["safe_attach_disclosures"], namespace["_attach_empty_disclosures"], namespace["requests"], namespace["st"]


class YanoshinTimeoutNonfatalTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        safe_attach_disclosures, attach_empty_disclosures, requests_module, st_module = load_yanoshin_helpers()
        cls.safe_attach_disclosures = staticmethod(safe_attach_disclosures)
        cls.attach_empty_disclosures = staticmethod(attach_empty_disclosures)
        cls.requests_module = requests_module
        cls.st_module = st_module

    def test_case_a_read_timeout_keeps_pts_candidates(self):
        df_in = pd.DataFrame(
            [
                {"code": "1111", "name": "候補A", "pct": 10.0, "volume": 1000},
                {"code": "2222", "name": "候補B", "pct": 9.0, "volume": 2000},
            ]
        )

        with patch.object(
            self.requests_module,
            "get",
            side_effect=self.requests_module.exceptions.ReadTimeout("read timed out"),
        ):
            out, err = self.safe_attach_disclosures(df_in, debug=False)

        self.assertEqual(err, "timeout")
        self.assertEqual(len(out), len(df_in))
        self.assertEqual(list(out["code"]), ["1111", "2222"])
        self.assertEqual(int(out.loc[0, "開示件数"]), 0)
        self.assertEqual(out.loc[0, "PDFリンク1"], "")

    def test_case_b_failed_disclosure_attach_adds_empty_columns(self):
        df_in = pd.DataFrame([{"code": "446A0", "name": "英字コード候補"}])

        out = self.attach_empty_disclosures(df_in)

        self.assertEqual(list(out["code"]), ["446A"])
        self.assertEqual(int(out.loc[0, "開示件数"]), 0)
        self.assertEqual(out.loc[0, "開示タイトル1"], "")
        self.assertEqual(out.loc[0, "PDFリンク1"], "")
        self.assertEqual(out.loc[0, "_開示上位5"], [])

    def test_case_c_debug_flag_does_not_change_candidate_row_count_on_timeout(self):
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
            with patch.object(self.st_module, "write"):
                out_debug_on, err_debug_on = self.safe_attach_disclosures(df_in, debug=True)

        self.assertEqual(err_debug_off, "timeout")
        self.assertEqual(err_debug_on, "timeout")
        self.assertEqual(len(out_debug_off), len(df_in))
        self.assertEqual(len(out_debug_on), len(df_in))
        self.assertEqual(list(out_debug_off["code"]), list(out_debug_on["code"]))

    def test_case_d_one_url_timeout_keeps_successful_disclosure_link(self):
        df_in = pd.DataFrame([{"code": "1111", "name": "候補A", "pct": 10.0, "volume": 1000}])
        yesterday_items = [
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
                return FakeResponse({"items": yesterday_items})
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach_disclosures(df_in, debug=False)

        self.assertEqual(err, "partial: timeout")
        self.assertEqual(len(out), len(df_in))
        self.assertEqual(int(out.loc[0, "開示件数"]), 1)
        self.assertEqual(out.loc[0, "PDFリンク1"], "https://example.com/docs/success.pdf")
        self.assertNotEqual(out.loc[0, "PDFリンク1"], "")

    def test_case_e_debug_flag_does_not_change_partial_success_result(self):
        df_in = pd.DataFrame([{"code": "1111", "name": "候補A", "pct": 10.0, "volume": 1000}])
        yesterday_items = [
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
                return FakeResponse({"items": yesterday_items})
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out_debug_off, err_debug_off = self.safe_attach_disclosures(df_in, debug=False)

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            with patch.object(self.st_module, "write"), patch.object(self.st_module, "dataframe"):
                out_debug_on, err_debug_on = self.safe_attach_disclosures(df_in, debug=True)

        self.assertEqual(err_debug_off, "partial: timeout")
        self.assertEqual(err_debug_on, "partial: timeout")
        self.assertEqual(len(out_debug_off), len(out_debug_on))
        self.assertEqual(int(out_debug_off.loc[0, "開示件数"]), int(out_debug_on.loc[0, "開示件数"]))
        self.assertEqual(out_debug_off.loc[0, "PDFリンク1"], out_debug_on.loc[0, "PDFリンク1"])


if __name__ == "__main__":
    unittest.main()
