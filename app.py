import re
import datetime
from typing import Optional, Tuple
from urllib.parse import urlsplit, urlunsplit, unquote

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup


APP_BUILD_ID = "yanoshin-partial-disclosures-20260511"
SBI_STOCK_DETAIL_URL_BASE = (
    "https://www.sbisec.co.jp/ETGate/WPLETsiR001Control/"
    "WPLETsiR001Ilst10/getDetailOfStockPriceJP"
)


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


def _make_sbi_stock_url(code: str) -> str:
    stock_code = _safe_text(code).upper()
    if stock_code == "":
        return ""
    return (
        f"{SBI_STOCK_DETAIL_URL_BASE}"
        f"?OutSide=on&exchange_code=JPN&getFlg=on&stock_sec_code_mul={stock_code}"
    )


def _make_sbi_stock_link_value(code: str, display_text: str) -> str:
    url = _make_sbi_stock_url(code)
    if url == "":
        return ""

    label = _safe_text(display_text)
    if label == "":
        label = _safe_text(code).upper()
    if label == "":
        return url
    return f"{url}#sbi_display_name={label}"


def _add_sbi_stock_links_for_display(df_in: pd.DataFrame) -> pd.DataFrame:
    df_out = df_in.copy()
    if "code" not in df_out.columns or "name" not in df_out.columns:
        return df_out

    df_out["name"] = df_out.apply(
        lambda row: _make_sbi_stock_link_value(row.get("code", ""), row.get("name", "")),
        axis=1,
    )
    return df_out


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


def _to_float_number(text: str) -> Optional[float]:
    if text is None:
        return None
    s = _safe_text(text).replace(",", "")
    if s == "":
        return None
    m = re.findall(r"[-+]?\d+(?:\.\d+)?", s)
    if len(m) == 0:
        return None
    try:
        return float(m[0])
    except Exception:
        return None


# ========= Yanoshin TDnet =========

def _normalize_company_code(s) -> str:
    s = _safe_text(s).upper()
    if s == "":
        return ""

    # 英字入りコードは 446A / 446A0 を 446A に正規化
    if re.search(r"[A-Za-z]", s):
        alnum = re.sub(r"[^A-Z0-9]", "", s)
        if re.fullmatch(r"\d{3}[A-Z]0", alnum):
            return alnum[:4]
        if re.fullmatch(r"\d{3}[A-Z]", alnum):
            return alnum
        return alnum

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

    def _canonicalize_document_url(raw_url) -> str:
        s = _safe_text(raw_url)
        if s == "":
            return ""

        try:
            parts = urlsplit(s)
        except Exception:
            return s.rstrip("/")

        scheme = parts.scheme.lower()
        netloc = parts.netloc.lower()
        path = parts.path.rstrip("/")
        if path == "" and parts.path.startswith("/"):
            path = "/"
        return urlunsplit((scheme, netloc, path, "", ""))

    def _extract_doc_identity(raw_url) -> str:
        canonical_url = _canonicalize_document_url(raw_url)
        if canonical_url == "":
            return ""

        try:
            path = urlsplit(canonical_url).path
        except Exception:
            path = ""

        normalized_path = unquote(path).rstrip("/")
        if normalized_path != "":
            return normalized_path
        return canonical_url

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

    def _fetch(url: str, source_tag: str) -> Tuple[pd.DataFrame, Optional[int], pd.DataFrame, Optional[str]]:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
        except requests.exceptions.RequestException as exc:
            return _normalize_yanoshin_df(pd.DataFrame(), source_tag=source_tag), None, pd.DataFrame(), _short_yanoshin_error_message(exc)

        items = _yanoshin_items_from_json(data)
        raw_df = pd.DataFrame(items)
        normalized_df = _normalize_yanoshin_df(raw_df, source_tag=source_tag)
        return normalized_df, int(r.status_code), raw_df, None

    td_today, status_today, raw_today, error_today = _fetch(url_today, source_tag="today")
    td_yesterday, status_yesterday, raw_yesterday, error_yesterday = _fetch(url_yesterday, source_tag="yesterday")
    fetch_errors = [err for err in [error_today, error_yesterday] if err is not None]
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
        td["canonical_url"] = td["document_url"].apply(_canonicalize_document_url)
        td["doc_identity"] = td["document_url"].apply(_extract_doc_identity)
        td["pub_date_only"] = td["pubdate"].apply(_extract_date_from_pubdate)

        td = td.drop_duplicates(subset=["code", "doc_identity"], keep="first")
    else:
        td = pd.DataFrame(
            columns=["code", "title", "document_url", "canonical_url", "doc_identity", "pubdate", "pub_date_only"]
        )
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
        st.write("【診断】Yanoshin取得成功件数:", int(2 - len(fetch_errors)))
        st.write("【診断】Yanoshin取得失敗件数:", int(len(fetch_errors)))
        if len(fetch_errors) > 0:
            st.write("【診断】Yanoshin取得失敗理由:", ", ".join(fetch_errors))
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
    if debug and len(td_today) > 0 and len(td) == 0:
        st.write(
            "【診断】Yanoshin normalize後の有効URLが0件です:",
            f"today={len(td_today)}, dropped_by_empty_filter={dropped_by_empty_filter}",
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

    pdf_cols = [f"PDFリンク{i+1}" for i in range(3)]
    attached_url_rows = int(df_out[pdf_cols].apply(lambda row: any(_safe_text(v) != "" for v in row), axis=1).sum())
    df_out.attrs["yanoshin_fetch_success_count"] = int(2 - len(fetch_errors))
    df_out.attrs["yanoshin_fetch_failed_count"] = int(len(fetch_errors))
    df_out.attrs["yanoshin_fetch_errors"] = fetch_errors
    df_out.attrs["yanoshin_fetched_disclosure_count"] = int(len(td_today) + len(td_yesterday))
    df_out.attrs["yanoshin_attached_url_rows"] = attached_url_rows
    if debug:
        st.write("【診断】Yanoshin attach後 PDFリンクあり行数:", attached_url_rows)

    return df_out


def _attach_empty_disclosures(df_in: pd.DataFrame) -> pd.DataFrame:
    df_out = df_in.copy()
    if "code" in df_out.columns:
        df_out["code"] = df_out["code"].apply(_normalize_company_code)

    df_out["開示件数"] = 0
    for i in range(3):
        df_out[f"開示タイトル{i+1}"] = ""
        df_out[f"PDFリンク{i+1}"] = ""

    df_out["_開示上位5"] = df_out.apply(lambda _row: [], axis=1)
    return df_out


def _short_yanoshin_error_message(exc: Exception) -> str:
    if isinstance(exc, requests.exceptions.Timeout):
        return "timeout"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "connection error"
    if isinstance(exc, requests.exceptions.RequestException):
        return "request error"
    return exc.__class__.__name__


def safe_attach_disclosures(df_in: pd.DataFrame, debug: bool = False) -> Tuple[pd.DataFrame, Optional[str]]:
    try:
        df_out = attach_disclosures(df_in, debug=debug)
    except requests.exceptions.RequestException as exc:
        message = _short_yanoshin_error_message(exc)
        if debug:
            st.write("【診断】Yanoshin開示取得: 失敗")
            st.write("【診断】Yanoshin開示取得失敗理由:", message)
        return _attach_empty_disclosures(df_in), message

    failed_count = int(df_out.attrs.get("yanoshin_fetch_failed_count", 0))
    fetched_count = int(df_out.attrs.get("yanoshin_fetched_disclosure_count", 0))
    fetch_errors = df_out.attrs.get("yanoshin_fetch_errors", [])
    reason = ", ".join(dict.fromkeys(fetch_errors)) if len(fetch_errors) > 0 else ""

    if failed_count > 0 and fetched_count > 0:
        message = f"partial: {reason}" if reason else "partial: request error"
        if debug:
            st.write("【診断】Yanoshin開示取得: 一部失敗")
        return df_out, message

    if failed_count > 0:
        message = reason if reason else "request error"
        if debug:
            st.write("【診断】Yanoshin開示取得: 失敗")
        return df_out, message

    if fetched_count == 0:
        if debug:
            st.write("【診断】Yanoshin開示取得: 0件")
        return df_out, "no disclosure data"

    return df_out, None


# ========= Kabutan PTS =========

PTS_URL_TEMPLATE = "https://s.kabutan.jp/warnings/pts_night_price_increase/?page={page}"

NORMAL_DAILY_PRICE_LIMITS = [
    (100, 30),
    (200, 50),
    (500, 80),
    (700, 100),
    (1000, 150),
    (1500, 300),
    (2000, 400),
    (3000, 500),
    (5000, 700),
    (7000, 1000),
    (10000, 1500),
    (15000, 3000),
    (20000, 4000),
    (30000, 5000),
    (50000, 7000),
    (70000, 10000),
    (100000, 15000),
    (150000, 30000),
    (200000, 40000),
    (300000, 50000),
    (500000, 70000),
    (700000, 100000),
    (1000000, 150000),
    (1500000, 300000),
    (2000000, 400000),
    (3000000, 500000),
    (5000000, 700000),
    (7000000, 1000000),
    (10000000, 1500000),
    (15000000, 3000000),
    (20000000, 4000000),
    (30000000, 5000000),
    (50000000, 7000000),
]


def fetch_pts_page(page: int) -> str:
    url = PTS_URL_TEMPLATE.format(page=page)
    # 403対策（必要な場合だけ効く）
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text


def _has_stop_high_marker(*texts: str) -> bool:
    for text in texts:
        compact = re.sub(r"\s+", "", _safe_text(text))
        if compact == "":
            continue
        if re.search(r"[SＳ](?:ｹ|ケ)?", compact):
            return True
    return False


def _get_normal_daily_price_limit(base_price: Optional[float]) -> Optional[float]:
    if base_price is None or pd.isna(base_price):
        return None
    try:
        price = float(base_price)
    except Exception:
        return None
    if price < 0:
        return None

    for upper_bound, limit_width in NORMAL_DAILY_PRICE_LIMITS:
        if price < float(upper_bound):
            return float(limit_width)
    return 10000000.0


def _is_stop_high_by_price(close_price: Optional[float], pts_price: Optional[float]) -> bool:
    if close_price is None or pts_price is None:
        return False
    if pd.isna(close_price) or pd.isna(pts_price):
        return False

    # Kabutan の S マーカーが欠ける場合の fallback。JPX の通常制限値幅のみを扱う。
    limit_width = _get_normal_daily_price_limit(close_price)
    if limit_width is None:
        return False

    stop_high_price = float(close_price) + float(limit_width)
    return float(pts_price) + 1e-9 >= stop_high_price


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
        m = re.search(r"(?<![A-Za-z0-9])(\d{4}|\d{3}[A-Za-z])(?![A-Za-z0-9])", th_text)
        if m is None:
            continue
        code = m.group(1).upper()
        name = th_text.replace(code, "").strip()

        close_price = _to_float_number(tds[0].get_text(strip=True))
        pts_price = _to_float_number(tds[1].get_text(strip=True))

        pct_raw = tds[2].get_text(strip=True)
        pct = _to_float_pct(pct_raw)

        volume = _to_int(tds[3].get_text(strip=True))

        # Kabutan は騰落率セルを複数 span に分けており、S / Sｹ が数値に密着して出ることがある。
        tds_text = " ".join([td.get_text(" ", strip=True) for td in tds])
        pct_text = tds[2].get_text(" ", strip=True)
        is_stop_high = _has_stop_high_marker(pct_raw, pct_text, tds_text) or _is_stop_high_by_price(
            close_price,
            pts_price,
        )

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


def filter_candidate_stocks(
    df: pd.DataFrame,
    pct_min: float,
    vol_min: int,
    ignore_volume_for_stop_high: bool,
) -> pd.DataFrame:
    df2 = df.dropna(subset=["pct"]).copy()

    is_stop_high = df2["is_stop_high"].fillna(False) == True
    passes_pct_filter = df2["pct"] >= float(pct_min)

    # 現在の repo にある「出来高系」閾値は volume のみ。
    # 将来、売買代金下限などを追加する場合もこの mask に寄せると、
    # 「ストップ高は出来高条件を無視」の適用範囲を同じ分岐で明示できる。
    passes_volume_filters = (df2["volume"] >= int(vol_min)).fillna(False)

    if ignore_volume_for_stop_high:
        volume_mask = is_stop_high | passes_volume_filters
    else:
        volume_mask = passes_volume_filters

    final_mask = passes_pct_filter & volume_mask
    return df2[final_mask].copy()


# ========= UI =========

st.set_page_config(layout="wide")
st.title("PTSナイトタイム上昇率ランキング + TDnet適時開示")

debug = st.checkbox("診断表示（開発用）", value=False)
st.caption("🟦＝当日　🟨＝前日（※Yanoshinのデータ内で最新日＝当日）")
if debug:
    st.caption(f"Build: {APP_BUILD_ID}")
    st.caption("診断メモ: 価格ベースのS高判定は通常制限値幅ベースです（臨時の制限値幅拡大は未対応）。")

pct_min = st.text_input("上昇率(%)の下限", value="5")
vol_min = st.text_input("出来高の下限", value="1000")
ignore_volume_for_stop_high = st.checkbox("ストップ高は出来高条件を無視", value=True)
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

        if debug:
            parsed_stop_high_count = 0
            if "is_stop_high" in df.columns and len(df) > 0:
                parsed_stop_high_count = int(df["is_stop_high"].fillna(False).sum())
            st.write("【診断】ビルド識別子:", APP_BUILD_ID)
            st.write("【診断】取得したPTS行数:", int(len(df)))
            st.write("【診断】parse直後 is_stop_high=True 件数:", parsed_stop_high_count)
            if len(df) > 0:
                st.write("【診断】parse直後 head(20):")
                st.dataframe(
                    df.reindex(
                        columns=["code", "name", "pct", "volume", "close_price", "pts_price", "is_stop_high"]
                    ).head(20),
                    hide_index=True,
                )

        df2 = filter_candidate_stocks(
            df=df,
            pct_min=float(pct_min_val),
            vol_min=int(vol_min_val),
            ignore_volume_for_stop_high=ignore_volume_for_stop_high,
        )

        if debug:
            filtered_stop_high_count = 0
            if "is_stop_high" in df2.columns and len(df2) > 0:
                filtered_stop_high_count = int(df2["is_stop_high"].fillna(False).sum())
            st.write("【診断】filter後の行数:", int(len(df2)))
            st.write("【診断】filter後 is_stop_high=True 件数:", filtered_stop_high_count)
            if len(df2) > 0:
                st.write("【診断】filter後 head(20):")
                st.dataframe(
                    df2.reindex(
                        columns=["code", "name", "pct", "volume", "close_price", "pts_price", "is_stop_high"]
                    ).head(20),
                    hide_index=True,
                )

        df2, disclosure_error = safe_attach_disclosures(df2, debug=debug)
        if disclosure_error is None:
            if debug:
                st.write("【診断】Yanoshin開示取得: 成功")
        elif disclosure_error.startswith("partial:"):
            st.warning("一部の適時開示取得に失敗しました。取得できた開示のみ表示しています。")
        else:
            st.warning("適時開示の取得に失敗しました。PTS一覧のみ表示しています。")

        hit = df2[df2["開示件数"] > 0].copy()
        st.write(f"【集計】開示あり: {len(hit)} / 開示なし: {len(df2) - len(hit)}")
        ignore_note = " ※ストップ高は出来高条件を無視" if ignore_volume_for_stop_high else ""
        st.success(
            f"{last_page}ページ目まで巡回。抽出 {len(df2)} 件（pct>={pct_min_val}, volume>={vol_min_val}{ignore_note}）"
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
        df_show_display = _add_sbi_stock_links_for_display(df_show)

        def _linkcol(colname: str):
            try:
                return st.column_config.LinkColumn(colname, display_text="PDF")
            except TypeError:
                return st.column_config.LinkColumn(colname)

        def _stock_linkcol(colname: str):
            try:
                return st.column_config.LinkColumn(colname, display_text=r"#sbi_display_name=(.*)$")
            except TypeError:
                return st.column_config.LinkColumn(colname)

        st.dataframe(
            df_show_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "name": _stock_linkcol("企業名"),
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

        













