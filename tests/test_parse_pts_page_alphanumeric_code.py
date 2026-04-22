import pathlib
import unittest


def load_pts_functions():
    app_path = pathlib.Path(__file__).resolve().parents[1] / "app.py"
    source = app_path.read_text(encoding="utf-8")
    source = source.split("# ========= UI =========", 1)[0]
    namespace = {}
    exec(compile(source, str(app_path), "exec"), namespace)
    return namespace["parse_pts_page"], namespace["filter_candidate_stocks"]


class ParsePtsPageAlphanumericCodeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        parse_pts_page, filter_candidate_stocks = load_pts_functions()
        cls.parse_pts_page = staticmethod(parse_pts_page)
        cls.filter_candidate_stocks = staticmethod(filter_candidate_stocks)

    def test_case_a_alphanumeric_code_row_is_parsed(self):
        html = """
        <div class="gray-sticky-table">
          <table>
            <tbody>
              <tr>
                <th>446A ノースサンド</th>
                <td>1,000</td>
                <td>1,100</td>
                <td>+10.0%</td>
                <td>12,345</td>
              </tr>
            </tbody>
          </table>
        </div>
        """

        out = self.parse_pts_page(html)

        self.assertEqual(len(out), 1)
        self.assertEqual(out.loc[0, "code"], "446A")
        self.assertEqual(out.loc[0, "name"], "ノースサンド")
        self.assertEqual(float(out.loc[0, "pct"]), 10.0)
        self.assertEqual(int(out.loc[0, "volume"]), 12345)
        self.assertFalse(bool(out.loc[0, "is_stop_high"]))

    def test_case_b_numeric_code_row_is_still_parsed(self):
        html = """
        <div class="gray-sticky-table">
          <table>
            <tbody>
              <tr>
                <th>1234 テスト銘柄</th>
                <td>2,000</td>
                <td>2,100</td>
                <td>+5.5%</td>
                <td>8,000</td>
              </tr>
            </tbody>
          </table>
        </div>
        """

        out = self.parse_pts_page(html)

        self.assertEqual(len(out), 1)
        self.assertEqual(out.loc[0, "code"], "1234")
        self.assertEqual(out.loc[0, "name"], "テスト銘柄")

    def test_case_c_alphanumeric_code_with_stop_high_marker_is_parsed(self):
        html = """
        <div class="gray-sticky-table">
          <table>
            <tbody>
              <tr>
                <th>446A ノースサンド</th>
                <td>1,000</td>
                <td>1,200</td>
                <td>+20.0% S</td>
                <td>15,000</td>
              </tr>
            </tbody>
          </table>
        </div>
        """

        out = self.parse_pts_page(html)

        self.assertEqual(len(out), 1)
        self.assertEqual(out.loc[0, "code"], "446A")
        self.assertTrue(bool(out.loc[0, "is_stop_high"]))

    def test_case_d_nested_pct_cell_with_attached_stop_high_marker_is_parsed(self):
        html = """
        <div class="gray-sticky-table">
          <table>
            <tbody>
              <tr>
                <th>446A ノースサンド</th>
                <td>1,000</td>
                <td>1,200</td>
                <td>
                  <span class="plus-num">+200</span><br>
                  <span class="text-2xs plus-num">+20.0<span class="text-[10px] ml-px">%</span><span class="text-red w-13px inline-block text-center text-xs">Ｓ</span></span>
                </td>
                <td>90</td>
              </tr>
            </tbody>
          </table>
        </div>
        """

        out = self.parse_pts_page(html)

        self.assertEqual(len(out), 1)
        self.assertEqual(out.loc[0, "code"], "446A")
        self.assertTrue(bool(out.loc[0, "is_stop_high"]))

    def test_case_e_alphanumeric_code_survives_filter_candidate_stocks(self):
        html = """
        <div class="gray-sticky-table">
          <table>
            <tbody>
              <tr>
                <th>446A ノースサンド</th>
                <td>1,000</td>
                <td>1,050</td>
                <td>+5.1%</td>
                <td>2,000</td>
              </tr>
            </tbody>
          </table>
        </div>
        """

        parsed = self.parse_pts_page(html)
        out = self.filter_candidate_stocks(
            df=parsed,
            pct_min=5.0,
            vol_min=1000,
            ignore_volume_for_stop_high=False,
        )

        self.assertEqual(list(out["code"]), ["446A"])


if __name__ == "__main__":
    unittest.main()
