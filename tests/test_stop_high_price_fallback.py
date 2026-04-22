import pathlib
import unittest


def load_pts_functions():
    app_path = pathlib.Path(__file__).resolve().parents[1] / "app.py"
    source = app_path.read_text(encoding="utf-8")
    source = source.split("# ========= UI =========", 1)[0]
    namespace = {}
    exec(compile(source, str(app_path), "exec"), namespace)
    return namespace["parse_pts_page"], namespace["filter_candidate_stocks"]


class StopHighPriceFallbackTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        parse_pts_page, filter_candidate_stocks = load_pts_functions()
        cls.parse_pts_page = staticmethod(parse_pts_page)
        cls.filter_candidate_stocks = staticmethod(filter_candidate_stocks)

    def test_case_a_price_limit_fallback_marks_stop_high_without_html_marker(self):
        html = """
        <div class="gray-sticky-table">
          <table>
            <tbody>
              <tr>
                <th>1111 価格到達銘柄</th>
                <td>1,000</td>
                <td>1,300</td>
                <td>+30.0%</td>
                <td>100</td>
              </tr>
            </tbody>
          </table>
        </div>
        """

        out = self.parse_pts_page(html)

        self.assertEqual(len(out), 1)
        self.assertTrue(bool(out.loc[0, "is_stop_high"]))

    def test_case_b_price_limit_fallback_supports_other_limit_band(self):
        html = """
        <div class="gray-sticky-table">
          <table>
            <tbody>
              <tr>
                <th>3333 別レンジ銘柄</th>
                <td>345</td>
                <td>425</td>
                <td>+23.2%</td>
                <td>90</td>
              </tr>
            </tbody>
          </table>
        </div>
        """

        out = self.parse_pts_page(html)

        self.assertEqual(len(out), 1)
        self.assertTrue(bool(out.loc[0, "is_stop_high"]))

    def test_case_c_low_volume_price_based_stop_high_survives_candidate_flow(self):
        html = """
        <div class="gray-sticky-table">
          <table>
            <tbody>
              <tr>
                <th>1111 価格到達銘柄</th>
                <td>1,000</td>
                <td>1,300</td>
                <td>+30.0%</td>
                <td>100</td>
              </tr>
              <tr>
                <th>2222 通常銘柄</th>
                <td>1,000</td>
                <td>1,250</td>
                <td>+25.0%</td>
                <td>100</td>
              </tr>
            </tbody>
          </table>
        </div>
        """

        parsed = self.parse_pts_page(html)
        filtered = self.filter_candidate_stocks(
            df=parsed,
            pct_min=5.0,
            vol_min=1000,
            ignore_volume_for_stop_high=True,
        )

        self.assertEqual(list(filtered["code"]), ["1111"])
        self.assertTrue(bool(filtered.loc[filtered.index[0], "is_stop_high"]))


if __name__ == "__main__":
    unittest.main()
