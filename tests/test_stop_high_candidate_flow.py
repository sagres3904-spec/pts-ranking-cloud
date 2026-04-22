import pathlib
import unittest


def load_pts_functions():
    app_path = pathlib.Path(__file__).resolve().parents[1] / "app.py"
    source = app_path.read_text(encoding="utf-8")
    source = source.split("# ========= UI =========", 1)[0]
    namespace = {}
    exec(compile(source, str(app_path), "exec"), namespace)
    return namespace["parse_pts_page"], namespace["filter_candidate_stocks"]


class StopHighCandidateFlowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        parse_pts_page, filter_candidate_stocks = load_pts_functions()
        cls.parse_pts_page = staticmethod(parse_pts_page)
        cls.filter_candidate_stocks = staticmethod(filter_candidate_stocks)

    def test_case_a_stop_high_survives_candidate_flow_with_low_volume(self):
        html = """
        <div class="gray-sticky-table">
          <table>
            <tbody>
              <tr>
                <th>1111 ストップ高銘柄</th>
                <td>1,000</td>
                <td>1,300</td>
                <td>
                  <span class="plus-num">+300</span><br>
                  <span class="text-2xs plus-num">+30.0<span class="text-[10px] ml-px">%</span><span class="text-red w-13px inline-block text-center text-xs">Ｓ</span></span>
                </td>
                <td>100</td>
              </tr>
              <tr>
                <th>2222 通常銘柄</th>
                <td>1,000</td>
                <td>1,250</td>
                <td>
                  <span class="plus-num">+250</span><br>
                  <span class="text-2xs plus-num">+25.0<span class="text-[10px] ml-px">%</span></span>
                </td>
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
        df_show = filtered.sort_values(
            by=["pct", "volume", "is_stop_high"],
            ascending=[False, False, False],
            kind="mergesort",
        ).reset_index(drop=True)

        self.assertEqual(list(df_show["code"]), ["1111"])
        self.assertTrue(bool(df_show.loc[0, "is_stop_high"]))


if __name__ == "__main__":
    unittest.main()
