import pathlib
import unittest
from unittest.mock import patch

import pandas as pd


def load_attach_disclosures_objects():
    app_path = pathlib.Path(__file__).resolve().parents[1] / "app.py"
    source = app_path.read_text(encoding="utf-8")
    source = source.split("# ========= UI =========", 1)[0]
    namespace = {}
    exec(compile(source, str(app_path), "exec"), namespace)
    return (
        namespace["_normalize_company_code"],
        namespace["attach_disclosures"],
        namespace["requests"],
    )


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class AttachDisclosuresAlphanumericCodeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        normalize_company_code, attach_disclosures, requests_module = load_attach_disclosures_objects()
        cls.normalize_company_code = staticmethod(normalize_company_code)
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

    def test_case_a_normalize_alphanumeric_code_keeps_four_chars(self):
        self.assertEqual(self.normalize_company_code("446A"), "446A")

    def test_case_b_normalize_alphanumeric_code_drops_trailing_zero(self):
        self.assertEqual(self.normalize_company_code("446A0"), "446A")

    def test_case_c_normalize_alphanumeric_code_uppercases_and_drops_zero(self):
        self.assertEqual(self.normalize_company_code("446a0"), "446A")

    def test_case_d_attach_disclosures_matches_446a_and_446a0(self):
        today = [
            {
                "company_code": "446A0",
                "title": "英字入りコードの開示",
                "document_url": "https://example.com/docs/446a.pdf",
                "pubdate": "2026-03-13 08:00",
            }
        ]

        out = self.run_attach(today, [], ["446A"])
        self.assertEqual(int(out.loc[0, "開示件数"]), 1)

    def test_case_e_attach_disclosures_matches_446a_and_lowercase_446a0(self):
        today = [
            {
                "company_code": "446a0",
                "title": "小文字コードの開示",
                "document_url": "https://example.com/docs/446a-lower.pdf",
                "pubdate": "2026-03-13 08:10",
            }
        ]

        out = self.run_attach(today, [], ["446A"])
        self.assertEqual(int(out.loc[0, "開示件数"]), 1)

    def test_case_f_numeric_code_still_matches_7203_and_72030(self):
        today = [
            {
                "company_code": "72030",
                "title": "数値コードの開示",
                "document_url": "https://example.com/docs/7203.pdf",
                "pubdate": "2026-03-13 08:20",
            }
        ]

        out = self.run_attach(today, [], ["7203"])
        self.assertEqual(int(out.loc[0, "開示件数"]), 1)


if __name__ == "__main__":
    unittest.main()
