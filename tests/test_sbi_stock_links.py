import pathlib
import unittest

import pandas as pd


def load_sbi_link_helpers():
    app_path = pathlib.Path(__file__).resolve().parents[1] / "app.py"
    source = app_path.read_text(encoding="utf-8")
    source = source.split("# ========= UI =========", 1)[0]
    namespace = {}
    exec(compile(source, str(app_path), "exec"), namespace)
    return (
        namespace["_make_sbi_stock_url"],
        namespace["_add_sbi_stock_links_for_display"],
    )


class SbiStockLinksTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        make_sbi_stock_url, add_sbi_stock_links_for_display = load_sbi_link_helpers()
        cls.make_sbi_stock_url = staticmethod(make_sbi_stock_url)
        cls.add_sbi_stock_links_for_display = staticmethod(add_sbi_stock_links_for_display)

    def test_case_a_make_sbi_stock_url_numeric_code(self):
        self.assertEqual(
            self.make_sbi_stock_url("7203"),
            "https://www.sbisec.co.jp/ETGate/WPLETsiR001Control/"
            "WPLETsiR001Ilst10/getDetailOfStockPriceJP"
            "?OutSide=on&exchange_code=JPN&getFlg=on&stock_sec_code_mul=7203",
        )

    def test_case_b_make_sbi_stock_url_uppercases_alphanumeric_code(self):
        self.assertIn("stock_sec_code_mul=446A", self.make_sbi_stock_url("446a"))

    def test_case_c_make_sbi_stock_url_handles_blank_values(self):
        self.assertEqual(self.make_sbi_stock_url(""), "")
        self.assertEqual(self.make_sbi_stock_url(None), "")

    def test_case_d_display_dataframe_uses_code_for_sbi_stock_link(self):
        df = pd.DataFrame(
            {
                "code": ["7203", "446a"],
                "name": ["トヨタ自動車", "ノースサンド"],
                "pct": ["5.00", "10.00"],
            }
        )

        out = self.add_sbi_stock_links_for_display(df)

        self.assertIn("stock_sec_code_mul=7203", out.loc[0, "name"])
        self.assertIn("#sbi_display_name=トヨタ自動車", out.loc[0, "name"])
        self.assertIn("stock_sec_code_mul=446A", out.loc[1, "name"])
        self.assertIn("#sbi_display_name=ノースサンド", out.loc[1, "name"])
        self.assertEqual(df.loc[0, "name"], "トヨタ自動車")


if __name__ == "__main__":
    unittest.main()
