from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


# =========================
# 路徑設定（以 factor-platform 為根目錄）
# =========================
APP_ROOT = Path(__file__).resolve().parents[1]          # factor-platform/
MERGED_DIR = APP_ROOT / "merged_csvs"
OUT_DIR = APP_ROOT / "data" / "returns"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# 讓 scripts/ 可以 import 專案根目錄的模組（alpha.py、clean_data.py 等）
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))


# =========================
# 小工具
# =========================
def safe_filename(name: str) -> str:
    """Windows 不允許的檔名字符替換掉"""
    name = re.sub(r'[\\/:*?"<>|]', "_", str(name)).strip()
    return name or "Factor"


def export_factor_json(name: str, s: pd.Series) -> Path:
    """輸出單一因子日報酬 JSON 到 data/returns/"""
    s = s.copy()
    s.index = pd.to_datetime(s.index, errors="coerce")
    s = s.sort_index()
    s = s.replace([np.inf, -np.inf], np.nan).dropna()

    obj = {
        "name": name,
        "dates": s.index.strftime("%Y-%m-%d").tolist(),
        "ret": s.astype(float).tolist(),
    }
    fp = OUT_DIR / f"{safe_filename(name)}.json"
    fp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    return fp


def load_csv(name: str) -> pd.DataFrame | None:
    """
    讀 merged_csvs/{name}.csv
    - price -> DatetimeIndex
    - 其餘 -> PeriodIndex('M')
    """
    fp = MERGED_DIR / f"{name}.csv"
    if not fp.exists():
        print(f"⚠ 找不到檔案：{fp}")
        return None

    df = pd.read_csv(fp, index_col=0, encoding="utf-8-sig")
    if name == "price":
        df.index = pd.to_datetime(df.index, errors="coerce")
        df.index.name = "date"
    else:
        df.index = pd.to_datetime(df.index, errors="coerce").to_period("M")
        df.index.name = "month"

    df.columns = df.columns.astype(str).str.strip()
    df = df.sort_index()
    print(f"✔ 已載入 {name} ({df.shape[0]} rows × {df.shape[1]} cols)")
    return df


def alp_return(alpha_df: pd.DataFrame, returns_df: pd.DataFrame, empty_as_zero: bool = True) -> pd.Series:
    """
    給定 alpha (0/1 矩陣) 和 returns，計算每日投組報酬（等權）。
    - 避免除以 0：當日無持股則回傳 0（或 NaN）
    """
    # 對齊 index/columns
    a = alpha_df.reindex(index=returns_df.index, columns=returns_df.columns).fillna(0.0)
    r = returns_df.reindex(index=a.index, columns=a.columns)

    weighted_ret = (a * r).sum(axis=1)
    counts = a.sum(axis=1).replace(0, np.nan)
    port = (weighted_ret / counts)

    if empty_as_zero:
        port = port.fillna(0.0)

    port.name = "ret"
    return port


# =========================
# 主流程
# =========================
def main():
    # ---- 1) 載入資料 ----
    var_names: List[str] = [
        "price", "mktcap", "pe_ratio", "pb_ratio", "yd",
        "beta", "earn_yoy", "gross", "rev", "eps"
    ]

    data: Dict[str, pd.DataFrame] = {}
    for name in var_names:
        df = load_csv(name)
        if df is not None:
            data[name] = df

    if "price" not in data or "mktcap" not in data:
        raise RuntimeError("至少需要 price.csv 與 mktcap.csv 才能計算因子報酬。")

    price = data["price"]
    returns = price.pct_change()

    mktcap = data["mktcap"]
    pe_ratio = data.get("pe_ratio")
    pb_ratio = data.get("pb_ratio")
    yd = data.get("yd")
    beta = data.get("beta")
    earn_yoy = data.get("earn_yoy")
    gross = data.get("gross")
    rev = data.get("rev")
    eps = data.get("eps")

    # ---- 2) 讀金融保險名單（你原本的 Excel）----
    # 注意：路徑以 factor-platform 為根目錄
    excel_fp = APP_ROOT / "因子資料全.xlsx"
    if not excel_fp.exists():
        raise FileNotFoundError(f"找不到 {excel_fp}（你用來排除金融股的 Excel）")

    finance_corp = pd.read_excel(excel_fp, sheet_name="金融保險（含下市櫃）")

    # ---- 3) import 你原本用的模組/函式 ----

    import alpha



    from alpha import (
        build_sample_pool,
        build_sample_pool_ex_fin,
        momentum_signal,
        pool_to_alpha,
        pe_low_signal,
        dy_high_signal,
        yoy_high_signal,
        margin_growth_signal,
        eps_growth_signal,
    )

    # ---- 4) 建樣本池 & 各因子 alpha ----
    # 你原本寫 top200 但 top_n=300；我保留你的行為
    top200 = build_sample_pool(mktcap, top_n=300)
    top200_nofin = build_sample_pool_ex_fin(mktcap, finance_corp)
    top200_alpha = pool_to_alpha(returns, top200)

    momentum_01_alpha = momentum_signal(returns, top200, lookback_months=1)
    momentum_03_alpha = momentum_signal(returns, top200, lookback_months=3)
    momentum_06_alpha = momentum_signal(returns, top200, lookback_months=6)

    if pe_ratio is None:
        raise RuntimeError("找不到 pe_ratio.csv，但你有用到 pe_low_signal。")
    pe_low_01_alpha = pe_low_signal(returns, pe_ratio, top200_nofin)

    if pb_ratio is None:
        raise RuntimeError("找不到 pb_ratio.csv，但你有用到 pb_low_01_alpha。")
    pb_low_01_alpha = pe_low_signal(returns, pb_ratio, top200_nofin)

    if yd is None:
        raise RuntimeError("找不到 yd.csv，但你有用到 dy_high_signal。")
    high_yield_alpha = dy_high_signal(returns, yd, top200, require_positive=False)

    # 你原本 low_vol_alpha 其實是用 beta 做 pe_low_signal（等於選 beta 最低）
    # 我照你的寫法保留
    if beta is None:
        raise RuntimeError("找不到 beta.csv，但你有用到 low_vol_alpha。")
    low_vol_alpha = pe_low_signal(returns, beta, top200, require_positive=True)

    if earn_yoy is None:
        raise RuntimeError("找不到 earn_yoy.csv，但你有用到 yoy_high_signal。")
    high_yoy_alpha = yoy_high_signal(
        returns,
        earn_yoy,
        top200,
        yoy_cap_ratio=200,
        yoy_is_percent=True,
        require_positive=False,
    )

    if gross is None or rev is None:
        raise RuntimeError("找不到 gross.csv 或 rev.csv，但你有用到 margin_growth_signal。")
    sig_margin = margin_growth_signal(
        returns=returns,
        gross=gross,
        operating=rev,
        mktcap_pool=top200_nofin,
    )

    if eps is None:
        raise RuntimeError("找不到 eps.csv，但你有用到 eps_growth_signal。")
    eps_up = eps_growth_signal(
        returns=returns,
        eps_est=eps,
        mktcap_pool=top200,
        increase_strict=True,
        require_positive=True,
    )

    # ---- 5) 計算各因子投組日報酬 ----
    ret_top200 = alp_return(top200_alpha, returns)

    ret_mom1 = alp_return(momentum_01_alpha, returns)
    ret_mom3 = alp_return(momentum_03_alpha, returns)
    ret_mom6 = alp_return(momentum_06_alpha, returns)

    ret_pe_low1 = alp_return(pe_low_01_alpha, returns)
    ret_pb_low1 = alp_return(pb_low_01_alpha, returns)

    ret_low_vol = alp_return(low_vol_alpha, returns)
    ret_high_yield = alp_return(high_yield_alpha, returns)
    ret_high_yoy = alp_return(high_yoy_alpha, returns)

    ret_rev_growth = alp_return(sig_margin, returns)
    ret_eps_growth = alp_return(eps_up, returns)

    # ---- 6) 輸出到 data/returns/*.json ----
    outputs: Dict[str, pd.Series] = {
        "Top200": ret_top200,
        "Momentum_01": ret_mom1,
        "Momentum_03": ret_mom3,
        "Momentum_06": ret_mom6,
        "PE_low": ret_pe_low1,
        "PB_low": ret_pb_low1,
        "Low_beta": ret_low_vol,          # 你原本叫 low_vol，但邏輯是 beta 最低
        "High_yield": ret_high_yield,
        "High_yoy": ret_high_yoy,
        "Margin_growth": ret_rev_growth,
        "EPS_growth": ret_eps_growth,
    }

    exported = []
    for name, s in outputs.items():
        fp = export_factor_json(name, s)
        exported.append(fp)

    print(f"\n✅ 匯出完成：{len(exported)} 檔 → {OUT_DIR}")
    for p in exported:
        print(" -", p.name)


if __name__ == "__main__":
    main()
