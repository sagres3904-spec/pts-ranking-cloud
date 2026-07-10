import datetime
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
        namespace["safe_attach_disclosures"],
        namespace["_determine_disclosure_target_dates"],
        namespace["_filter_code_backfill_to_target_dates"],
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


class CodeBackfillDateWindowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        (
            safe_attach_disclosures,
            determine_target_dates,
            filter_code_backfill,
            requests_module,
            st_module,
        ) = load_disclosure_helpers()
        cls.safe_attach_disclosures = staticmethod(safe_attach_disclosures)
        cls.determine_target_dates = staticmethod(determine_target_dates)
        cls.filter_code_backfill = staticmethod(filter_code_backfill)
        cls.requests_module = requests_module
        cls.st_module = st_module

    def run_attach(self, df_in, today_items, yesterday_items, code_items_by_marker=None, debug=False):
        if code_items_by_marker is None:
            code_items_by_marker = {}

        def fake_get(url, timeout=20):
            if "today.json2" in url:
                return FakeResponse({"items": today_items})
            if "yesterday.json2" in url:
                return FakeResponse({"items": yesterday_items})
            for marker, items in code_items_by_marker.items():
                if marker in url:
                    return FakeResponse({"items": items})
            return FakeResponse({"items": []})

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            if debug:
                with patch.object(self.st_module, "write"), patch.object(self.st_module, "dataframe"):
                    return self.safe_attach_disclosures(df_in, debug=True)
            return self.safe_attach_disclosures(df_in, debug=False)

    def base_today_yesterday_items(self):
        return (
            [
                {
                    "company_code": "1111",
                    "title": "基準日 today",
                    "document_url": "https://example.com/base/today.pdf",
                    "pubdate": "2026-07-10 15:00",
                }
            ],
            [
                {
                    "company_code": "2222",
                    "title": "基準日 yesterday",
                    "document_url": "https://example.com/base/yesterday.pdf",
                    "pubdate": "2026-07-09 15:00",
                }
            ],
        )

    def birdman_code_items(self):
        items = [
            {
                "company_code": "7063",
                "title": "Birdman 2026-07-10 A",
                "document_url": "https://example.com/docs/7063-20260710-a.pdf",
                "pubdate": "2026-07-10 15:30",
            },
            {
                "company_code": "7063",
                "title": "Birdman 2026-07-10 B",
                "document_url": "https://example.com/docs/7063-20260710-b.pdf",
                "pubdate": "2026-07-10 15:31",
            },
            {
                "company_code": "7063",
                "title": "Birdman 2026-07-09",
                "document_url": "https://example.com/docs/7063-20260709.pdf",
                "pubdate": "2026-07-09 15:30",
            },
        ]
        for i in range(44):
            day = 7 - (i % 6)
            items.append(
                {
                    "company_code": "7063",
                    "title": f"Birdman old {i}",
                    "document_url": f"https://example.com/docs/7063-old-{i}.pdf",
                    "pubdate": f"2026-07-{day:02d} 15:00",
                }
            )
        return items

    def test_birdman_raw_47_is_filtered_to_target_3(self):
        today, yesterday = self.base_today_yesterday_items()
        df_in = pd.DataFrame([{"code": "7063", "name": "Birdman"}])

        out, err = self.run_attach(df_in, today, yesterday, {"list/7063.json2": self.birdman_code_items()})

        self.assertIsNone(err)
        self.assertEqual(int(out.loc[0, "開示件数"]), 3)
        self.assertEqual(len(out.loc[0, "_開示上位5"]), 3)
        info = out.attrs["yanoshin_debug_info"]
        self.assertEqual(info["birdman_code_specific_raw_count"], 47)
        self.assertEqual(info["birdman_code_specific_filtered_count"], 3)
        self.assertEqual(info["code_specific_out_of_target_date_count"], 44)

    def test_birdman_keeps_two_20260710_pdfs_and_one_20260709_pdf(self):
        today, yesterday = self.base_today_yesterday_items()
        df_in = pd.DataFrame([{"code": "7063", "name": "Birdman"}])

        out, _err = self.run_attach(df_in, today, yesterday, {"list/7063.json2": self.birdman_code_items()})

        links = [out.loc[0, f"PDFリンク{i}"] for i in [1, 2, 3]]
        self.assertEqual(len(set(links)), 3)
        self.assertIn("https://example.com/docs/7063-20260710-a.pdf", links)
        self.assertIn("https://example.com/docs/7063-20260710-b.pdf", links)
        self.assertIn("https://example.com/docs/7063-20260709.pdf", links)
        top5_text = "\n".join(item["title"] for item in out.loc[0, "_開示上位5"])
        self.assertIn("Birdman 2026-07-10 A", top5_text)
        self.assertIn("Birdman 2026-07-10 B", top5_text)
        self.assertIn("Birdman 2026-07-09", top5_text)
        self.assertNotIn("Birdman old", top5_text)

    def test_same_pdf_is_still_deduped_after_date_filter(self):
        today, yesterday = self.base_today_yesterday_items()
        df_in = pd.DataFrame([{"code": "7063", "name": "Birdman"}])
        code_items = [
            {
                "company_code": "7063",
                "title": "同一PDF A",
                "document_url": "https://example.com/docs/7063-dup.pdf?token=a",
                "pubdate": "2026-07-10 15:30",
            },
            {
                "company_code": "7063",
                "title": "同一PDF B",
                "document_url": "https://example.com/docs/7063-dup.pdf?token=b",
                "pubdate": "2026-07-10 15:31",
            },
        ]

        out, _err = self.run_attach(df_in, today, yesterday, {"list/7063.json2": code_items})

        self.assertEqual(int(out.loc[0, "開示件数"]), 1)
        self.assertEqual(out.loc[0, "PDFリンク2"], "")

    def test_signpost_missing_same_day_backfill_survives_but_old_disclosures_do_not(self):
        df_in = pd.DataFrame([{"code": "3996", "name": "サインポスト"}])
        today = [
            {
                "company_code": "3996",
                "title": "業績予想の修正及び配当予想の修正に関するお知らせ",
                "document_url": "https://example.com/docs/3996-forecast.pdf",
                "pubdate": "2026-07-10 15:30",
            }
        ]
        code_items = [
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
            {
                "company_code": "3996",
                "title": "古いサインポスト開示",
                "document_url": "https://example.com/docs/3996-old.pdf",
                "pubdate": "2026-07-07 15:30",
            },
        ]

        out, err = self.run_attach(df_in, today, [], {"list/3996.json2": code_items})

        self.assertIsNone(err)
        self.assertEqual(int(out.loc[0, "開示件数"]), 2)
        top5_text = "\n".join(item["title"] for item in out.loc[0, "_開示上位5"])
        self.assertIn("業績予想の修正及び配当予想の修正に関するお知らせ", top5_text)
        self.assertIn("2027年2月期 第1四半期決算短信", top5_text)
        self.assertNotIn("古いサインポスト開示", top5_text)

    def test_one_base_date_uses_that_date_and_previous_day(self):
        td_today = pd.DataFrame([{"pubdate": "2026-07-10 15:00"}])
        td_yesterday = pd.DataFrame()

        target_dates = self.determine_target_dates(td_today, td_yesterday)

        self.assertEqual(
            target_dates,
            {datetime.date(2026, 7, 10), datetime.date(2026, 7, 9)},
        )

    def test_zero_base_dates_uses_injected_jst_today_and_previous_day(self):
        target_dates = self.determine_target_dates(
            pd.DataFrame(),
            pd.DataFrame(),
            jst_today=datetime.date(2026, 7, 11),
        )

        self.assertEqual(
            target_dates,
            {datetime.date(2026, 7, 11), datetime.date(2026, 7, 10)},
        )

    def test_empty_or_unparseable_pubdate_backfill_rows_are_excluded(self):
        today, yesterday = self.base_today_yesterday_items()
        df_in = pd.DataFrame([{"code": "7063", "name": "Birdman"}])
        code_items = [
            {
                "company_code": "7063",
                "title": "対象日",
                "document_url": "https://example.com/docs/7063-good.pdf",
                "pubdate": "2026-07-10 15:30",
            },
            {
                "company_code": "7063",
                "title": "空pubdate",
                "document_url": "https://example.com/docs/7063-empty-date.pdf",
                "pubdate": "",
            },
            {
                "company_code": "7063",
                "title": "不正pubdate",
                "document_url": "https://example.com/docs/7063-bad-date.pdf",
                "pubdate": "not-a-date",
            },
        ]

        out, _err = self.run_attach(df_in, today, yesterday, {"list/7063.json2": code_items})

        self.assertEqual(int(out.loc[0, "開示件数"]), 1)
        self.assertEqual(out.attrs["yanoshin_debug_info"]["code_specific_unparseable_date_count"], 2)

    def test_alphanumeric_code_backfill_survives_date_filter(self):
        today, yesterday = self.base_today_yesterday_items()
        df_in = pd.DataFrame([{"code": "446A"}, {"code": "130A"}])
        code_items = [
            {
                "company_code": "446A0",
                "title": "446A 対象日",
                "document_url": "https://example.com/docs/446a-target.pdf",
                "pubdate": "2026-07-10 15:30",
            },
            {
                "company_code": "130A",
                "title": "130A 対象日",
                "document_url": "https://example.com/docs/130a-target.pdf",
                "pubdate": "2026-07-09 15:30",
            },
            {
                "company_code": "446A0",
                "title": "446A 古い開示",
                "document_url": "https://example.com/docs/446a-old.pdf",
                "pubdate": "2026-07-07 15:30",
            },
        ]

        out, _err = self.run_attach(df_in, today, yesterday, {"list/446A-130A.json2": code_items})

        counts = dict(zip(out["code"], out["開示件数"]))
        self.assertEqual(int(counts["446A"]), 1)
        self.assertEqual(int(counts["130A"]), 1)

    def test_debug_true_and_false_keep_same_disclosure_outputs(self):
        today, yesterday = self.base_today_yesterday_items()
        df_in = pd.DataFrame([{"code": "7063", "name": "Birdman"}])
        code_items = {"list/7063.json2": self.birdman_code_items()}

        out_debug_off, err_debug_off = self.run_attach(df_in, today, yesterday, code_items, debug=False)
        out_debug_on, err_debug_on = self.run_attach(df_in, today, yesterday, code_items, debug=True)

        self.assertIsNone(err_debug_off)
        self.assertIsNone(err_debug_on)
        for col in ["開示件数", "PDFリンク1", "PDFリンク2", "PDFリンク3", "_開示上位5"]:
            self.assertEqual(out_debug_off.loc[0, col], out_debug_on.loc[0, col], col)

    def test_code_specific_timeout_keeps_today_yesterday_success(self):
        df_in = pd.DataFrame([{"code": "7063", "name": "Birdman"}])
        today = [
            {
                "company_code": "7063",
                "title": "全体取得成功",
                "document_url": "https://example.com/docs/7063-global.pdf",
                "pubdate": "2026-07-10 15:30",
            }
        ]

        def fake_get(url, timeout=20):
            if "today.json2" in url:
                return FakeResponse({"items": today})
            if "yesterday.json2" in url:
                return FakeResponse({"items": []})
            if "list/7063.json2" in url:
                raise self.requests_module.exceptions.ReadTimeout("read timed out")
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(self.requests_module, "get", side_effect=fake_get):
            out, err = self.safe_attach_disclosures(df_in, debug=False)

        self.assertIsNone(err)
        self.assertEqual(int(out.loc[0, "開示件数"]), 1)
        self.assertEqual(out.loc[0, "PDFリンク1"], "https://example.com/docs/7063-global.pdf")


if __name__ == "__main__":
    unittest.main()
