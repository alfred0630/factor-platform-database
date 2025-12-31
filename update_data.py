
import pandas as pd
import numpy as np

import re

import importlib
import clean_data



import pandas as pd
import re

def clean_price(df):
    """
    清理每日收盤價表：
    - 從第一欄提取 YYYYMMDD
    - 設為 index
    - 只留下股票代號欄位
    """
    df = df.copy()
    
    # 第一欄名稱（應該是 '股票代號'）
    first_col = df.columns[0]
    
    # 從文字中抽出 YYYYMMDD
    df["date"] = df[first_col].astype(str).str.extract(r"(\d{8})", expand=False)
    df = df.dropna(subset=["date"])

    # 設為索引
    df = df.set_index("date")
    df.index.name = "date"
    
    # 股票代號欄位（4~6位數字）
    code_cols = [c for c in df.columns if re.fullmatch(r"\d{4,6}", str(c))]
    df = df[code_cols].apply(pd.to_numeric, errors="coerce")

    return df


def clean_code_table_ready(df):
    """
    清洗表格：
      - index 轉成 YYYYMM（2025Q1 -> 202501）
      - columns 為股票代號（1101, 1102, ...）
    """
    df = df.copy()
    
    # 1️⃣ 找出股票代號欄
    code_cols = [c for c in df.columns if re.fullmatch(r"\d{4,6}", str(c))]
    if not code_cols:
        raise ValueError("找不到股票代號欄（4~6位數）。")

    first_col = df.columns[0]

    # 2️⃣ 抽取期別並轉換 Q1→01、Q2→02、Q3→03、Q4→04
    def extract_period(s):
        s = str(s)
        m = re.search(r"(\d{4})Q([1-4])", s)
        if m:
            year, q = m.group(1), m.group(2)
            return f"{year}0{q}"  # 2025Q1 → 202501
        m = re.search(r"(\d{6})", s)  # 月資料
        if m:
            return m.group(1)
        return None

    df["period"] = df[first_col].map(extract_period)
    df = df.dropna(subset=["period"])

    # 3️⃣ 數值化
    df[code_cols] = df[code_cols].apply(pd.to_numeric, errors="coerce")

    # 4️⃣ 以 period 聚合
    out = df.groupby("period", as_index=True)[code_cols].mean().sort_index()
    out.index.name = "period"
    out.columns = [str(c) for c in out.columns]
    return out


def clean_eps(df):
    """
    清洗EPS格式資料：
    - 保留所有期別 (202508、202509、202510...)
    - index 為年月
    - 欄位為股票代號
    """
    df = df.copy()

    # ✅ 不要用第一列當欄名，直接保留所有資料
    # 改成用第 0 欄當 period 來源
    first_col = df.columns[0]
    
    # 取出年月
    df["period"] = df[first_col].astype(str).str.extract(r"(\d{6})", expand=False)
    df = df.dropna(subset=["period"])

    # 設 index
    df = df.set_index("period")
    df.index.name = "period"

    # 股票代號欄位：4~6 位數字
    code_cols = [c for c in df.columns if re.fullmatch(r"\d{4,6}", str(c))]
    df = df[code_cols].apply(pd.to_numeric, errors="coerce")

    return df
import re
import pandas as pd

def to_ym_by_code(df):
    """
    df 形狀同你截圖：
      第一欄標題為 '股票代號'，
      其餘欄為 1101、1102...，
      列標示如 '20250829本益比'。
    回傳：index=YYYYMM, columns=股票代碼
    """
    df = df.copy()
    df = df.iloc[:,4:].drop(index=0,axis=0)

    # 1) 抓第一欄（日期+指標字串），萃取 YYYYMM
    first_col = df.columns[0]              # '股票代號'
    ym = df[first_col].astype(str).str.extract(r'(\d{6})', expand=False)
    mask = ym.notna()
    ym = ym[mask].astype(int)

    # 2) 只保留 4~6 位數的股票代碼欄
    code_cols = [c for c in df.columns if re.fullmatch(r'\d{4,6}', str(c))]
    if not code_cols:
        raise ValueError("找不到股票代碼欄（4~6位數）。")

    # 3) 取出數值並轉型
    values = df.loc[mask, code_cols].apply(pd.to_numeric, errors='coerce')

    # 4) 設年月為索引；若同月重複，取平均
    values.index = ym.values
    out = values.groupby(values.index).mean().sort_index()
    out.index.name = "period"
    # 欄名統一成字串（可要可不要）
    out.columns = [str(c) for c in out.columns]
    return out

# 使用：
# res = to_ym_by_code(df)
# res.head()
# res.to_csv("cleaned.csv", encoding="utf-8-sig")
# res.to_excel("cleaned.xlsx")

import pandas as pd

# === 因子資料 ===
pe_df      = pd.read_excel("更新因子.xlsx", sheet_name="本益比")
pb_df      = pd.read_excel("更新因子.xlsx", sheet_name="pb")
yields_df  = pd.read_excel("更新因子.xlsx", sheet_name="殖利率")
beta_df    = pd.read_excel("更新因子.xlsx", sheet_name="Beta")
mv_df      = pd.read_excel("更新因子.xlsx", sheet_name="市值_")

pe_new     = to_ym_by_code(pe_df)
pb_new     = to_ym_by_code(pb_df)
beta_new   = to_ym_by_code(beta_df)
mv_new     = to_ym_by_code(mv_df)
yields_new = to_ym_by_code(yields_df)

# === 收盤價 ===
price_df = pd.read_excel("更新因子.xlsx", sheet_name="收盤價")
cleaned_price_new = clean_price(price_df.iloc[:, 4:].drop(index=0, axis=0))

# === EPS ===
eps_df = pd.read_excel("更新因子.xlsx", sheet_name="預估eps")
cleaned_eps_new = clean_eps(eps_df.iloc[:, 4:].drop(index=0, axis=0))

# === 毛利率與營業利益率 ===
gross_df = pd.read_excel("更新因子.xlsx", sheet_name="毛利率")
rev_df   = pd.read_excel("更新因子.xlsx", sheet_name="營業利益率")

gross_new = clean_code_table_ready(gross_df.iloc[:, 4:].drop(index=0, axis=0))
rev_new   = clean_code_table_ready(rev_df.iloc[:, 4:].drop(index=0, axis=0))

# === 月營收 ===
rev_month_df = pd.read_excel("更新因子.xlsx", sheet_name="月營收")
rev_month_new = clean_eps(rev_month_df.iloc[:, 4:].drop(index=0, axis=0))


# === 月資料：轉成 YYYY-MM ===
pe_new.index = pe_new.index.astype(str).str.strip().str[:4] + "-" + pe_new.index.astype(str).str.strip().str[4:6]
pb_new.index = pb_new.index.astype(str).str.strip().str[:4] + "-" + pb_new.index.astype(str).str.strip().str[4:6]
yields_new.index = yields_new.index.astype(str).str.strip().str[:4] + "-" + yields_new.index.astype(str).str.strip().str[4:6]
beta_new.index = beta_new.index.astype(str).str.strip().str[:4] + "-" + beta_new.index.astype(str).str.strip().str[4:6]
mv_new.index = mv_new.index.astype(str).str.strip().str[:4] + "-" + mv_new.index.astype(str).str.strip().str[4:6]

cleaned_eps_new.index = cleaned_eps_new.index.astype(str).str.strip().str[:4] + "-" + cleaned_eps_new.index.astype(str).str.strip().str[4:6]
gross_new.index = gross_new.index.astype(str).str.strip().str[:4] + "-" + gross_new.index.astype(str).str.strip().str[4:6]
rev_new.index = rev_new.index.astype(str).str.strip().str[:4] + "-" + rev_new.index.astype(str).str.strip().str[4:6]
rev_month_new.index = rev_month_new.index.astype(str).str.strip().str[:4] + "-" + rev_month_new.index.astype(str).str.strip().str[4:6]

# === 日資料：保留原日期格式 ===
cleaned_price_new.index = pd.to_datetime(cleaned_price_new.index.astype(str).str.strip(), errors="coerce")

print("✔ 月資料已轉為 YYYY-MM；cleaned_price_new 保留日期格式")


import pandas as pd
import numpy as np
import re
import importlib
import clean_data

# 重新載入自訂模組（確保是最新版本）
importlib.reload(clean_data)
from clean_data import clean_mktcap, clean_price

# === 匯入原始 Excel 各工作表 ===
price_raw      = pd.read_excel("因子資料全.xlsx", sheet_name="收盤價")        # 原始收盤價
mktcap_raw     = pd.read_excel("因子資料全.xlsx", sheet_name="市值")         # 市值
pe_raw         = pd.read_excel("因子資料全.xlsx", sheet_name="低本益比")     # 本益比
pb_raw         = pd.read_excel("因子資料全.xlsx", sheet_name="低PB")         # 淨值比
yield_raw      = pd.read_excel("因子資料全.xlsx", sheet_name="殖利率")       # 殖利率
beta_raw       = pd.read_excel("因子資料全.xlsx", sheet_name="Beta")         # Beta

earning_raw    = pd.read_excel("因子資料全.xlsx", sheet_name="月營收")       # 月營收
gross_raw      = pd.read_excel("因子資料全.xlsx", sheet_name="毛利率")      # 毛利率
rev_raw        = pd.read_excel("因子資料全.xlsx", sheet_name="營利率")      # 營業利益率

finance_raw    = pd.read_excel("因子資料全.xlsx", sheet_name="金融保險（含下市櫃）")  # 金融保險
eps_raw        = pd.read_excel("因子資料全.xlsx", sheet_name="月預估EPS")    # 預估 EPS

# === 清洗與命名 ===

price    = clean_price(price_raw)
mktcap   = clean_mktcap(mktcap_raw)
pe_ratio = clean_mktcap(pe_raw)
pb_ratio = clean_mktcap(pb_raw)
yd       = clean_mktcap(yield_raw)
beta     = clean_mktcap(beta_raw)
earn_yoy = clean_mktcap(earning_raw)
gross    = clean_mktcap(gross_raw)
rev      = clean_mktcap(rev_raw)
eps      = clean_mktcap(eps_raw)

# === 額外衍生變數 ===
returns  = price.pct_change()  # 報酬率矩陣（日線）

print("✔ 所有因子已載入並清洗完成")
import pandas as pd

VAR_MAP = {
    "pe_new":            "pe_ratio",
    "pb_new":            "pb_ratio",
    "yields_new":        "yd",
    "beta_new":          "beta",
    "mv_new":            "mktcap",
    "cleaned_price_new": "price",     # 日資料
    "cleaned_eps_new":   "eps",
    "gross_new":         "gross",
    "rev_new":           "rev",
    "rev_month_new":     "earn_yoy",
}

def _as_dt_index_and_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # 若有 month/period 欄位就設為 index
    if out.index.dtype == object and str(out.index.name).lower() in ["month", "period"]:
        pass
    else:
        for cand in ["month", "period"]:
            if cand in out.columns:
                out = out.set_index(cand)
                break

    # 轉 datetime（支援 YYYYMM）
    idx_str = out.index.astype(str).str.strip()
    mask_yyyymm = idx_str.str.fullmatch(r"\d{6}")
    idx_dt = pd.to_datetime(pd.Series(idx_str.where(~mask_yyyymm, idx_str + "01"),
                                      index=out.index),
                            errors="coerce")
    out.index = idx_dt

    # 去時區、normalize、去重
    if getattr(out.index, "tz", None) is not None:
        out.index = out.index.tz_localize(None)
    out.index = out.index.normalize()
    if not out.index.is_unique:
        out = out.groupby(level=0).last()

    # 欄名清理
    out.columns = out.columns.map(lambda x: str(x).strip())
    if out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated(keep="last")]
    return out.sort_index()

def merge_update_df(dst: pd.DataFrame, src: pd.DataFrame) -> pd.DataFrame:
    """以 src 非 NA 覆蓋 dst；回傳 DatetimeIndex（不轉字串）。"""
    dst = _as_dt_index_and_cols(dst)
    src = _as_dt_index_and_cols(src)

    all_idx = dst.index.union(src.index)
    all_col = dst.columns.union(src.columns)

    dst2 = dst.reindex(index=all_idx, columns=all_col)
    src2 = src.reindex(index=all_idx, columns=all_col)

    return dst2.where(src2.isna(), src2)  # 來源優先覆蓋

# === 產出 merged，不改動原始變數 ===
merged = {}
log = []

for src_name, dst_name in VAR_MAP.items():
    g = globals()
    if src_name in g and dst_name in g \
       and isinstance(g[src_name], pd.DataFrame) \
       and isinstance(g[dst_name], pd.DataFrame):

        before_shape = g[dst_name].shape
        res = merge_update_df(g[dst_name], g[src_name])

        # ✅ 在這裡決定 index 輸出格式：price 保留 YYYY-MM-DD，其餘 YYYY-MM
        if isinstance(res.index, pd.DatetimeIndex):
            if dst_name == "price":
                res.index = res.index.strftime("%Y-%m-%d")
            else:
                res.index = res.index.strftime("%Y-%m")

        after_shape = res.shape
        merged[dst_name] = res
        log.append(f"✔ {src_name} + {dst_name} → merged['{dst_name}']  {before_shape} → {after_shape}")
    else:
        log.append(f"… 略過 {src_name} -> {dst_name}（其中一邊不存在或不是 DataFrame）")

print("\n".join(log))


import pandas as pd
import os

# === 設定輸出資料夾 ===
output_folder = "merged_csvs"
os.makedirs(output_folder, exist_ok=True)

# === 建立要輸出的字典 ===
output_dict = merged.copy()

# 若有想補的 raw 資料
raw_vars = ["finance_raw"]

g = globals()
for var in raw_vars:
    if var.replace("_raw", "") not in output_dict and var in g and isinstance(g[var], pd.DataFrame):
        df = g[var]
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, errors="ignore")
        output_dict[var.replace("_raw", "")] = df

# === 每個 DataFrame 各存成一個 CSV（index 格式化） ===
for name, df in output_dict.items():
    df_to_save = df.copy()

    # ✅ 根據名稱決定 index 格式
    if isinstance(df_to_save.index, pd.DatetimeIndex):
        if name == "price":
            df_to_save.index = df_to_save.index.strftime("%Y-%m-%d")  # 收盤價保留日
        else:
            df_to_save.index = df_to_save.index.strftime("%Y-%m")      # 其他只保留年月

    file_path = os.path.join(output_folder, f"{name}.csv")
    df_to_save.to_csv(file_path, encoding="utf-8-sig")
    print(f"✔ 已輸出：{file_path}")

print(f"\n✅ 全部完成，共輸出 {len(output_dict)} 個 CSV 檔到資料夾：{output_folder}")
