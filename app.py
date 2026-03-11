import re
import datetime
from typing import Optional, Tuple

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup


# ========= ユーティリティ =========

def _safe_text(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip()
    if s.lower() == "nan":
        return ""
    return s


def _to_int(text: str) -> Optional[int]:
    if text is None:
        return None
    t = str(text).strip()
    if t == "":
        return None
    t = t.replace(",", "")
    digits = "".join([c for c in t if c.isdigit()])
    return int(digits) if digits != "" else None


def _to_float_pct(pct_text: str) -> Optional[float]:
    if pct_text is None:
        return None
    s = str(pct_text).replace("％", "%")
    m = re.findall(r"[-+]?\d+(?:\.\d+)?\s*%", s)
    if len(m) > 0:
        x = m[0].replace("%", "").strip()
        try:
            return float(x)
        except Exception:
            return None
    m2 = re.findall(r"[-+]?\d+(?:\.\d+)?", s)
    if len(m2) > 0:
        try:
            return float(m2[0])
        except Exception:
            return None
    return None


# ========= Yanoshin TDnet =========

def _normalize_company_code(s) -> str:
    s = _safe_text(s)
    if s == "":
        return ""

    # 英字入りコードはそのまま維持（例: 130A）
    if re.search(r"[A-Za-z]", s):
        return s

    # 数値コードは「5桁末尾0を落とす」→「4桁は4桁で維持」に統一
    digits = re.sub(r"\D", "", s)
    if digits == "":
        return ""
    if len(digits) == 5 and digits.endswith("0"):
        return digits[:4]
    if len(digits) == 4:
        return digits
    if len(digits) < 4:
        return digits.zfill(4)
    return digits[:4]


def _extract_date_from_pubdate(pubdate: str) -> Optional[datetime.date]:
    s = _safe_text(pubdate)
    if s == "":
        return None

    m = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        try:
            return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None

    m2 = re.search(r"(\d{4})(\d{2})(\d{2})", s)
    if m2:
        try:
            return datetime.date(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
        except Exception:
            return None

    return None


def attach_disclosures(df_in: pd.DataFrame, debug: bool = False) -> pd.DataFrame:
    url_today = "https://webapi.yanoshin.jp/webapi/tdnet/list/today.json2?limit=2000"
    url_yesterday = "https://webapi.yanoshin.jp/webapi/tdnet/list/yesterday.json2?limit=2000"

    def _pick_value(it: dict, *keys) -> str:
        if not isinstance(it, dict):
            return ""

        for k in keys:
            v = it.get(k)
            if v is not None and _safe_text(v) != "":
                return v

        normalized = {}
        for k, v in it.items():
            nk = re.sub(r"[^a-z0-9]", "", str(k).lower())
            normalized[nk] = v

        for k in keys:
            nk = re.sub(r"[^a-z0-9]", "", str(k).lower())
            if nk in normalized and _safe_text(normalized[nk]) != "":
                return normalized[nk]
        return ""

    def _normalize_code_value(raw_code) -> str:
        if raw_code is None:
            return ""
        try:
            if isinstance(raw_code, float) and pd.isna(raw_code):
                return ""
            if isinstance(raw_code, (int, float)):
                return _normalize_company_code(str(int(float(raw_code))))
        except Exception:
            pass

        s = _safe_text(raw_code)
        if s == "":
            return ""
        try:
            f = float(s)
            if pd.notna(f):
                return _normalize_company_code(str(int(f)))
        except Exception:
            pass
        return _normalize_company_code(s)

    def _normalize_yanoshin_df(raw_df: pd.DataFrame, source_tag: str) -> pd.DataFrame:
        expanded_df = raw_df.copy()
        if "Tdnet" in expanded_df.columns:
            tdnet_rows = []
            for _, row in expanded_df.iterrows():
                tdnet_val = row.get("Tdnet")
                if isinstance(tdnet_val, dict):
                    tdnet_rows.append(tdnet_val)
                else:
                    tdnet_rows.append({})
            expanded_df = pd.json_normalize(tdnet_rows)

        rows = []
        for _, row in expanded_df.iterrows():
            it = row.to_dict() if hasattr(row, "to_dict") else dict(row)
            raw_code = _pick_value(it, "company_code", "code", "CompanyCode", "Company_Code")
            raw_title = _pick_value(it, "title", "Title", "subject", "Subject")
            raw_url = _pick_value(it, "document_url", "documentUrl", "pdf_url", "pdfUrl", "url", "Url")
            raw_pubdate = _pick_value(it, "pubdate", "Pubdate", "date", "Date", "published_at")

            rows.append(
                {
                    "code": _normalize_code_value(raw_code),
                    "source_tag": source_tag,
                    "title": _safe_text(raw_title),
                    "document_url": _safe_text(raw_url),
                    "pubdate": _safe_text(raw_pubdate),
                }
            )

        if len(rows) == 0:
            return pd.DataFrame(columns=["code", "source_tag", "title", "document_url", "pubdate"])
        return pd.DataFrame(rows)

    def _yanoshin_items_from_json(data) -> list:
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("items")
            if items is None:
                items = data.get("result")
            if items is None:
                items = data.get("Tdnet")
            if items is None:
                items = []
        else:
            items = []

        if not isinstance(items, list):
            items = []

        flat_items = []
        for item in items:
            if isinstance(item, dict) and isinstance(item.get("Tdnet"), dict):
                flat_items.append(item.get("Tdnet"))
            else:
                flat_items.append(item)
        return flat_items

    def _fetch(url: str, source_tag: str) -> Tuple[pd.DataFrame, int, pd.DataFrame]:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()

        items = _yanoshin_items_from_json(data)
        raw_df = pd.DataFrame(items)
        normalized_df = _normalize_yanoshin_df(raw_df, source_tag=source_tag)
        return normalized_df, int(r.status_code), raw_df

    td_today, status_today, raw_today = _fetch(url_today, source_tag="today")
    td_yesterday, status_yesterday, raw_yesterday = _fetch(url_yesterday, source_tag="yesterday")
    td = pd.concat([td_today, td_yesterday], ignore_index=True)

    if len(td) > 0:
        td["code"] = td["code"].apply(_normalize_company_code)
        td["document_url"] = td["document_url"].apply(_safe_text)
        td["title"] = td["title"].apply(_safe_text)
        td["pubdate"] = td["pubdate"].apply(_safe_text)

        required_cols = ["code", "document_url"]
        missing_cols = [c for c in required_cols if c not in td.columns]
        if len(missing_cols) > 0:
            if debug:
                st.write("【診断】空フィルタ前の不足列:", missing_cols)
                st.write("【診断】空フィルタ前のcolumns:", td.columns.tolist())
                st.write("【診断】空フィルタ前のhead(3):")
                st.dataframe(td.head(3))
            st.error(f"Yanoshinデータの必須列が不足しています: {missing_cols}")
            raise RuntimeError(f"Yanoshin required columns missing: {missing_cols}")

        before_filter = len(td)
        td = td[(td["code"] != "") & (td["document_url"] != "")].copy()
        after_filter = len(td)
        dropped_by_empty_filter = before_filter - after_filter
        td["pub_date_only"] = td["pubdate"].apply(_extract_date_from_pubdate)

        td = td.drop_duplicates(subset=["code", "document_url"], keep="first")
    else:
        td = pd.DataFrame(columns=["code", "title", "document_url", "pubdate", "pub_date_only"])
        dropped_by_empty_filter = 0

    # 返ってきた中で最新の日付＝当日、次点＝前日
    max_date = None
    prev_date = None
    if len(td) > 0:
        dates = [d for d in td["pub_date_only"].tolist() if isinstance(d, datetime.date)]
        uniq = sorted(set(dates), reverse=True)
        if len(uniq) >= 1:
            max_date = uniq[0]
        if len(uniq) >= 2:
            prev_date = uniq[1]

    def _day_tag(d: Optional[datetime.date]) -> str:
        if d is None:
            return ""
        if max_date is not None and d == max_date:
            return "当日"
        if prev_date is not None and d == prev_date:
            return "前日"
        return ""

    if len(td) > 0:
        td["day_tag"] = td["pub_date_only"].apply(_day_tag)
    else:
        td["day_tag"] = ""

    if debug:
        st.write("【診断】取得URL:", url_today, url_yesterday)
        st.write("【診断】status code today/yesterday:", status_today, status_yesterday)
        st.write("【診断】Yanoshin件数 today:", int(len(td_today)))
        st.write("【診断】Yanoshin件数 yesterday:", int(len(td_yesterday)))
        st.write("【診断】today raw shape:", tuple(raw_today.shape))
        st.write("【診断】today columns:", raw_today.columns.tolist())
        if list(raw_today.columns) == ["Tdnet"] and len(raw_today) > 0:
            td0 = raw_today["Tdnet"].iloc[0]
            st.write("【診断】today Tdnet先頭要素type:", str(type(td0)))
            st.write("【診断】today Tdnet先頭要素(短縮):", str(td0)[:300])
        st.write("【診断】yesterday raw shape:", tuple(raw_yesterday.shape))
        st.write("【診断】yesterday columns:", raw_yesterday.columns.tolist())
        st.write("【診断】today normalize後 shape:", tuple(td_today.shape))
        st.write("【診断】today normalize後 columns:", td_today.columns.tolist())
        st.write("【診断】today codeサンプル10件:", td_today.get("code", pd.Series(dtype=object)).head(10).tolist())
        st.write("【診断】today urlサンプル3件:", td_today.get("document_url", pd.Series(dtype=object)).head(3).tolist())
        st.write("【診断】today head(3):")
        st.dataframe(td_today.head(3))
        st.write("【診断】yesterday head(3):")
        st.dataframe(td_yesterday.head(3))
        st.write("【診断】空フィルタ後の件数:", int(len(td_today) + len(td_yesterday) - dropped_by_empty_filter))
        st.write("【診断】normalize後の件数:", int(len(td_today) + len(td_yesterday)))
        st.write("【診断】Yanoshin結合後件数（重複除去後）:", int(len(td)))
        if len(td_today) > 0 and len(td) == 0:
            st.error(
                f"【診断】todayが0件超なのに結合後が0件です。空フィルタで{dropped_by_empty_filter}件除外されました。"
            )
    if len(td_today) > 0 and len(td) == 0:
        raise RuntimeError(
            f"Yanoshin normalize failed: today={len(td_today)} > 0 but merged=0. dropped_by_empty_filter={dropped_by_empty_filter}"
        )

    if debug and len(raw_today) > 0:
        today_code_samples = []
        for _, r in raw_today.head(10).iterrows():
            it = r.to_dict()
            raw_code = _pick_value(it, "company_code", "code", "CompanyCode", "Company_Code")
            today_code_samples.append(_normalize_code_value(raw_code))
        today_url_samples = []
        for _, r in raw_today.head(3).iterrows():
            it = r.to_dict()
            today_url_samples.append(_pick_value(it, "document_url", "documentUrl", "pdf_url", "pdfUrl", "url", "Url"))
        st.write("【診断】today codeサンプル10件(候補キー適用後):", today_code_samples)
        st.write("【診断】today urlサンプル3件(候補キー適用後):", today_url_samples)

    if debug and len(td_today) > 0:
        empty_url_count = int((td_today["document_url"] == "").sum())
        st.write("【診断】today URL空件数:", empty_url_count)
        st.write("【診断】today URL有効件数:", int(len(td_today) - empty_url_count))
        if len(td) > 0:
            st.write("【診断】pubdateサンプル先頭10:", td["pubdate"].dropna().head(10).tolist())
            uniq_dates = sorted(
                set([d for d in td["pub_date_only"].tolist() if isinstance(d, datetime.date)]),
                reverse=True,
            )
            st.write("【診断】pub_date_onlyユニーク（新しい順）:", [str(x) for x in uniq_dates])
            st.write("【診断】当日とみなす日付:", str(max_date) if max_date else "なし")
            st.write("【診断】前日とみなす日付:", str(prev_date) if prev_date else "なし")

    def _rank(tag: str) -> int:
        if tag == "当日":
            return 0
        if tag == "前日":
            return 1
        return 9

    def _decorate_title(day_tag: str, title: str) -> str:
        title = _safe_text(title)
        if day_tag == "当日":
            prefix = "🟦 "
        elif day_tag == "前日":
            prefix = "🟨 "
        else:
            prefix = ""
        if title == "":
            return prefix + "(タイトルなし)"
        return prefix + title

    by_code = {}
    if len(td) > 0:
        td2 = td.copy()
        td2["rank"] = td2["day_tag"].apply(_rank)
        td2 = td2.sort_values(by=["code", "rank"], ascending=True)

        for _, row in td2.iterrows():
            c = _normalize_company_code(row.get("code", ""))
            day_tag = _safe_text(row.get("day_tag", ""))
            title_text = _decorate_title(day_tag, row.get("title", ""))
            url = _safe_text(row.get("document_url", ""))
            by_code.setdefault(c, []).append((day_tag, title_text, url))

    df_out = df_in.copy()
    df_out["code"] = df_out["code"].apply(_normalize_company_code)

    df_out["開示件数"] = df_out["code"].apply(lambda c: len(by_code.get(c, [])))

    def _get_item(c, i):
        items = by_code.get(c, [])
        if i < len(items):
            _dt, title_text, url = items[i]
            return title_text, url
        return "", ""

    for i in range(3):
        df_out[f"開示タイトル{i+1}"] = df_out["code"].apply(lambda c, i=i: _get_item(c, i)[0])
        df_out[f"PDFリンク{i+1}"] = df_out["code"].apply(lambda c, i=i: _get_item(c, i)[1])

    def _top5(c):
        items = by_code.get(c, [])[:5]
        return [{"title": t, "url": u} for (_dt, t, u) in items]

    df_out["_開示上位5"] = df_out["code"].apply(_top5)

    return df_out


# ========= Kabutan PTS =========

PTS_URL_TEMPLATE = "https://s.kabutan.jp/warnings/pts_night_price_increase/?page={page}"


def fetch_pts_page(page: int) -> str:
    url = PTS_URL_TEMPLATE.format(page=page)
    # 403対策（必要な場合だけ効く）
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text


def parse_pts_page(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("div.gray-sticky-table table")
    if table is None:
        return pd.DataFrame()

    tbody = table.find("tbody")
    if tbody is None:
        return pd.DataFrame()

    rows = []
    for tr in tbody.find_all("tr"):
        th = tr.find("th")
        tds = tr.find_all("td")
        if th is None or len(tds) < 4:
            continue

        th_text = th.get_text(" ", strip=True)
        m = re.search(r"(\d{4})", th_text)
        if m is None:
            continue
        code = m.group(1)
        name = th_text.replace(code, "").strip()

        close_price = _to_int(tds[0].get_text(strip=True))
        pts_price = _to_int(tds[1].get_text(strip=True))

        pct_raw = tds[2].get_text(strip=True)
        pct = _to_float_pct(pct_raw)

        volume = _to_int(tds[3].get_text(strip=True))

        # ストップ高表示（S または Sｹ）が「セル内に独立して」出ている時だけ True
        tds_text = " ".join([td.get_text(" ", strip=True) for td in tds])
        is_stop_high = bool(re.search(r"(?:^|\s)[SＳ](?:ｹ|ケ)?(?:$|\s)", tds_text))

        rows.append(
            {
                "code": code,
                "name": name,
                "pct": pct,
                "volume": volume,
                "close_price": close_price,
                "pts_price": pts_price,
                "pct_raw": pct_raw,
                "is_stop_high": is_stop_high,
            }
        )

    return pd.DataFrame(rows)


def crawl_until_below_threshold(pct_threshold: float, max_pages: int, debug: bool = False) -> Tuple[pd.DataFrame, int]:
    all_df = []
    last_page = 0

    for page in range(1, max_pages + 1):
        html = fetch_pts_page(page)
        df = parse_pts_page(html)
        last_page = page

        if df is None or len(df) == 0:
            if debug:
                st.write(f"【診断】Kabutan page={page}: 0件（停止）")
            break

        mx = None
        if "pct" in df.columns:
            mx0 = df["pct"].max(skipna=True)
            if mx0 is not None and not pd.isna(mx0):
                try:
                    mx = float(mx0)
                except Exception:
                    mx = None

        all_df.append(df)

        if debug:
            st.write(f"【診断】Kabutan page={page}: max pct =", mx if mx is not None else "None/NaN")

        if mx is None or mx < pct_threshold:
            break

    if len(all_df) == 0:
        return pd.DataFrame(), last_page

    out = pd.concat(all_df, ignore_index=True)
    return out, last_page


# ========= UI =========

st.set_page_config(layout="wide")
st.title("PTSナイトタイム上昇率ランキング + TDnet適時開示")

debug = st.checkbox("診断表示（開発用）", value=False)
st.caption("🟦＝当日　🟨＝前日（※Yanoshinのデータ内で最新日＝当日）")

pct_min = st.text_input("上昇率(%)の下限", value="5")
vol_min = st.text_input("出来高の下限", value="1000")
max_pages = st.text_input("最大ページ数（安全のため）", value="30")

if st.button("取得して表示"):
    try:
        pct_min_val = _to_float_pct(pct_min)
        vol_min_val = _to_int(vol_min)
        max_pages_val = _to_int(max_pages)

        if pct_min_val is None:
            raise ValueError(f"上昇率(%)の下限が解釈できません: {pct_min}")
        if vol_min_val is None:
            raise ValueError(f"出来高の下限が解釈できません: {vol_min}")
        if max_pages_val is None or max_pages_val <= 0:
            raise ValueError(f"最大ページ数が解釈できません: {max_pages}")

        df, last_page = crawl_until_below_threshold(
            pct_threshold=float(pct_min_val),
            max_pages=int(max_pages_val),
            debug=debug,
        )

        df2 = df.dropna(subset=["pct", "volume"]).copy()

        # 出来高条件（ストップ高は出来高条件を無視）
        df2 = df2[
            (df2["pct"] >= float(pct_min_val)) &
            ((df2["volume"] >= int(vol_min_val)) | (df2["is_stop_high"] == True))
        ].copy()

        df2 = attach_disclosures(df2, debug=debug)

        hit = df2[df2["開示件数"] > 0].copy()
        st.write(f"【集計】開示あり: {len(hit)} / 開示なし: {len(df2) - len(hit)}")
        st.success(
            f"{last_page}ページ目まで巡回。抽出 {len(df2)} 件（pct>={pct_min_val}, volume>={vol_min_val} ※ストップ高は出来高条件を無視）"
        )

        df_show = df2.sort_values(
            by=["pct", "volume", "is_stop_high"],
            ascending=[False, False, False],
            kind="mergesort",
        ).reset_index(drop=True)

        if debug:
            st.write("【診断】表示直前ソート確認 head(10):")
            st.dataframe(df_show[["pct", "volume", "is_stop_high"]].head(10), hide_index=True)

        df_show["pct"] = df_show["pct"].apply(lambda x: "" if pd.isna(x) else f"{float(x):.2f}")
        df_show["volume"] = df_show["volume"].apply(lambda x: "" if pd.isna(x) else f"{int(x):,}")
        df_show["開示件数"] = df_show["開示件数"].apply(lambda x: "" if pd.isna(x) else str(int(x)))

        cols = [
            "code", "name", "pct", "volume",
            "開示件数",
            "開示タイトル1", "PDFリンク1",
            "開示タイトル2", "PDFリンク2",
            "開示タイトル3", "PDFリンク3",
        ]
        df_show = df_show.reindex(columns=cols)

        def _linkcol(colname: str):
            try:
                return st.column_config.LinkColumn(colname, display_text="PDF")
            except TypeError:
                return st.column_config.LinkColumn(colname)

        st.dataframe(
            df_show,
            use_container_width=True,
            hide_index=True,
            column_config={
                "PDFリンク1": _linkcol("PDFリンク1"),
                "PDFリンク2": _linkcol("PDFリンク2"),
                "PDFリンク3": _linkcol("PDFリンク3"),
            },
        )

        # 詳細（最大5件）
        for _, row in hit.iterrows():
            code = row.get("code", "")
            name = row.get("name", "")
            items = row.get("_開示上位5", [])
            total = int(row.get("開示件数", 0))

            note = ""
            if total > 5:
                note = f"（全{total}件のうち上位5件のみ表示）"

            with st.expander(f"{code} {name} の適時開示 {note}"):
                for it in items:
                    t = _safe_text(it.get("title", ""))
                    u = _safe_text(it.get("url", ""))
                    if u:
                        st.markdown(f"- [{t}]({u})")
                    else:
                        st.markdown(f"- {t}")

    except Exception as e:
        st.error(f"取得に失敗しました: {e}")
else:
    st.info("条件を設定して「取得して表示」を押してください。")

        













