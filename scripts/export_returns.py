from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


# =========================
# 路徑設定（以 factor-platform 為根目錄）
# =========================
APP_ROOT = Path(__file__).resolve().parents[1]          # factor-platform/
MERGED_DIR = APP_ROOT / "merged_csvs"

RET_OUT_DIR = APP_ROOT / "data" / "returns"
META_OUT_DIR = APP_ROOT / "data" / "factors"
HOLD_OUT_DIR = APP_ROOT / "data" / "holdings"
MANIFEST_FP = APP_ROOT / "data" / "manifest.json"

RET_OUT_DIR.mkdir(parents=True, exist_ok=True)
META_OUT_DIR.mkdir(parents=True, exist_ok=True)
HOLD_OUT_DIR.mkdir(parents=True, exist_ok=True)

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
    fp = RET_OUT_DIR / f"{safe_filename(name)}.json"
    fp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    return fp


def export_meta_json(name: str, meta: dict) -> Path:
    """輸出因子 meta JSON 到 data/factors/"""
    # 確保 meta 裡 factor 一致
    meta = dict(meta)
    meta.setdefault("factor", name)

    fp = META_OUT_DIR / f"{safe_filename(name)}.json"
    fp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
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
    a = alpha_df.reindex(index=returns_df.index, columns=returns_df.columns).fillna(0.0)
    r = returns_df.reindex(index=a.index, columns=a.columns)

    weighted_ret = (a * r).sum(axis=1)
    counts = a.sum(axis=1).replace(0, np.nan)
    port = (weighted_ret / counts)

    if empty_as_zero:
        port = port.fillna(0.0)

    port.name = "ret"
    return port


def alpha_to_monthly_holdings(alpha_df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    把日頻 alpha(0/1) 轉成「每月持股名單」：
    - 每個月用『該月最後一個交易日』的 alpha 來代表該月持股
    - 輸出: {"YYYY-MM": ["2330","2317",...], ...}
    """
    a = alpha_df.copy()
    a.index = pd.to_datetime(a.index, errors="coerce")
    a = a.sort_index()
    a.columns = a.columns.astype(str).str.strip()

    month_key = a.index.to_period("M")

    holdings: Dict[str, List[str]] = {}
    for m in month_key.unique():
        mask = (month_key == m)
        if not mask.any():
            continue
        last_day = a.index[mask][-1]
        row = a.loc[last_day]
        picks = row[row.astype(float) > 0].index.astype(str).tolist()
        holdings[str(m)] = picks

    return holdings


def export_holdings_json(name: str, alpha_df: pd.DataFrame) -> Path:
    """
    輸出 holdings JSON 到 data/holdings/{factor}.json
    """
    h = alpha_to_monthly_holdings(alpha_df)
    months = sorted(h.keys())

    obj = {
        "factor": name,
        "asof": pd.Timestamp(alpha_df.index.max()).strftime("%Y-%m-%d") if len(alpha_df.index) else None,
        "months": months,
        "holdings": h
    }

    fp = HOLD_OUT_DIR / f"{safe_filename(name)}.json"
    fp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return fp


def export_manifest(outputs: Dict[str, pd.Series], meta_registry: Optional[Dict[str, dict]] = None) -> Path:
    """
    產 manifest.json，前端拿來列因子/判斷 detail 是否可用
    """
    factors = list(outputs.keys())
    has_detail = []
    if meta_registry:
        for f in factors:
            if f in meta_registry:
                has_detail.append(f)

    obj = {
        "factors": factors,
        "has_detail": has_detail,
        "generated_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    MANIFEST_FP.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return MANIFEST_FP


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

    # ✅ meta registry（你已經貼到 alpha.py 了）
    FACTOR_META_REGISTRY = getattr(alpha, "FACTOR_META_REGISTRY", {})

    # ---- 4) 建 pool / alpha 訊號 ----
    top200 = build_sample_pool(mktcap, top_n=200)
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

    # ---- 5.5) Benchmark：加權指數 ----
    # ⚠ 你原本是寫死 C: 路徑，這裡改成：優先用 APP_ROOT/更新因子.xlsx，找不到才用原本路徑
    tw_fp1 = APP_ROOT / "更新因子.xlsx"
    tw_fp2 = Path("C:/Users/admin/Desktop/factor-platform/更新因子.xlsx")

    tw_fp = tw_fp1 if tw_fp1.exists() else tw_fp2
    if not tw_fp.exists():
        raise FileNotFoundError(f"找不到加權指數檔案：{tw_fp1} 或 {tw_fp2}")

    tw = pd.read_excel(tw_fp, sheet_name="加權指數")
    tw = tw.iloc[4:, 1:]
    tw.columns = ["date", "twa00"]
    tw = tw.set_index("date")
    ret_twa00 = tw.pct_change().dropna()["twa00"]

    # ---- 6) 輸出到 data/returns/*.json ----
    outputs: Dict[str, pd.Series] = {
        "Top200": ret_top200,
        "Momentum_01": ret_mom1,
        "Momentum_03": ret_mom3,
        "Momentum_06": ret_mom6,
        "PE_low": ret_pe_low1,
        "PB_low": ret_pb_low1,
        "Low_beta": ret_low_vol,
        "High_yield": ret_high_yield,
        "High_yoy": ret_high_yoy,
        "Margin_growth": ret_rev_growth,
        "EPS_growth": ret_eps_growth,
        "TWA00": ret_twa00,
    }

    exported = []
    for name, s in outputs.items():
        fp = export_factor_json(name, s)
        exported.append(fp)

    print(f"\n✅ returns 匯出完成：{len(exported)} 檔 → {RET_OUT_DIR}")

    # ---- 7) 輸出 factors meta（因子說明）----
    meta_exported = []
    for name in outputs.keys():
        meta = FACTOR_META_REGISTRY.get(name)
        if meta is None:
            print(f"⚠ factors meta 找不到：{name}（alpha.py 的 FACTOR_META_REGISTRY 沒收錄）")
            continue
        fp = export_meta_json(name, meta)
        meta_exported.append(fp)

    print(f"✅ factors(meta) 匯出完成：{len(meta_exported)} 檔 → {META_OUT_DIR}")

    # ---- 8) 輸出 holdings（每月持股名單，可回看）----
    # 這裡用「實際 alpha 矩陣」去做 holdings，跟 returns 完全一致
    alpha_outputs: Dict[str, pd.DataFrame] = {
        "Top200": top200_alpha,
        "Momentum_01": momentum_01_alpha,
        "Momentum_03": momentum_03_alpha,
        "Momentum_06": momentum_06_alpha,
        "PE_low": pe_low_01_alpha,
        "PB_low": pb_low_01_alpha,
        "Low_beta": low_vol_alpha,
        "High_yield": high_yield_alpha,
        "High_yoy": high_yoy_alpha,
        "Margin_growth": sig_margin,
        "EPS_growth": eps_up,
        # TWA00 沒有 holdings（指數）
    }

    hold_exported = []
    for name, a in alpha_outputs.items():
        fp = export_holdings_json(name, a)
        hold_exported.append(fp)

    print(f"✅ holdings 匯出完成：{len(hold_exported)} 檔 → {HOLD_OUT_DIR}")

    # ---- 9) 輸出 manifest.json ----
    mf = export_manifest(outputs, meta_registry=FACTOR_META_REGISTRY)
    print(f"✅ manifest 匯出完成：{mf}")

    # ---- 10) 印出檔名 ----
    print("\n📦 匯出清單：")
    for p in exported:
        print(" - returns:", p.name)
    for p in meta_exported:
        print(" - factors:", p.name)
    for p in hold_exported:
        print(" - holdings:", p.name)
    print(" - manifest:", mf.name)


if __name__ == "__main__":
    main()
