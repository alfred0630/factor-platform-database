import numpy as np
import pandas as pd



# =========================
# Factor METADATA (copy/paste)
# Matches your alpha.py implementations
# =========================

TOP200_META = {
    "factor": "Top200",
    "display_name": "Top200 市值池",
    "category": "Universe",
    "rebalance": "M",
    "universe": "Top200",
    "holding_rule": "以當月市值排序取前 200 檔，作為『下個月』投資宇宙；持有月內等權持有該宇宙",
    "params": {
        "top_n": 200,
        "source_func": "build_sample_pool / pool_to_alpha",
        "timing": "pool[ym+1] = TopN(mktcap at ym)"
    },
    "timing_notes": "你在 build_sample_pool 明確用『當月市值→下月宇宙』，因此宇宙決策不前視；pool_to_alpha 會把 pool[m] 標記到 (m+1) 月交易日"
}

MOMENTUM_01_META = {
    "factor": "Momentum_01",
    "display_name": "動能（回看 1 個月）",
    "category": "Momentum",
    "rebalance": "M",
    "universe": "Top200",
    "holding_rule": (
        "在『當月 m』的 Top200 宇宙內，計算『回看 1 個月（含當月）』日報酬的幾何累積報酬；"
        "選取報酬排名前 30% 的股票，並只保留累積報酬 > 0；"
        "於『下個月 m+1』整月等權持有"
    ),
    "params": {
        "top_frac": 0.30,
        "lookback_months": 1,
        "require_positive_momentum": True,
        "source_func": "momentum_signal"
    },
    "timing_notes": "momentum_signal 以『當月 m 的資料』決定『下月 m+1 持有』；並額外濾除動能 <= 0 的股票"
}

MOMENTUM_03_META = {
    "factor": "Momentum_03",
    "display_name": "動能（回看 3 個月）",
    "category": "Momentum",
    "rebalance": "M",
    "universe": "Top200",
    "holding_rule": (
        "在『當月 m』的 Top200 宇宙內，計算『回看 3 個月（含當月）』日報酬的幾何累積報酬；"
        "選取報酬排名前 30% 的股票，並只保留累積報酬 > 0；"
        "於『下個月 m+1』整月等權持有"
    ),
    "params": {
        "top_frac": 0.30,
        "lookback_months": 3,
        "require_positive_momentum": True,
        "source_func": "momentum_signal"
    },
    "timing_notes": "momentum_signal 以『當月 m』挑選，配置到『下月 m+1』"
}

MOMENTUM_06_META = {
    "factor": "Momentum_06",
    "display_name": "動能（回看 6 個月）",
    "category": "Momentum",
    "rebalance": "M",
    "universe": "Top200",
    "holding_rule": (
        "在『當月 m』的 Top200 宇宙內，計算『回看 6 個月（含當月）』日報酬的幾何累積報酬；"
        "選取報酬排名前 30% 的股票，並只保留累積報酬 > 0；"
        "於『下個月 m+1』整月等權持有"
    ),
    "params": {
        "top_frac": 0.30,
        "lookback_months": 6,
        "require_positive_momentum": True,
        "source_func": "momentum_signal"
    },
    "timing_notes": "momentum_signal 以『當月 m』挑選，配置到『下月 m+1』"
}

PE_LOW_META = {
    "factor": "PE_low",
    "display_name": "低本益比（PE）",
    "category": "Value",
    "rebalance": "M",
    "universe": "Top200 ex-fin (由 pool 決定)",
    "holding_rule": (
        "以『上月 prev_m = m-1』的 PE 橫切面，在『上月的 TopN 宇宙（pool[prev_m]）』內排序；"
        "若 require_positive=True 則只保留 PE>0；"
        "取 PE 最低的 bottom_frac=30%；"
        "於『本月 m』整月等權持有"
    ),
    "params": {
        "bottom_frac": 0.30,
        "require_positive": True,
        "source_func": "pe_low_signal",
        "universe_key_used": "pool[prev_m] (prev_m=m-1)",
        "decision_month": "m-1",
        "holding_month": "m"
    },
    "timing_notes": "pe_low_signal 明確用上月指標決定本月持有；避免前視偏誤"
}

PB_LOW_META = {
    "factor": "PB_low",
    "display_name": "低股價淨值比（PB）",
    "category": "Value",
    "rebalance": "M",
    "universe": "Top200 ex-fin (由 pool 決定)",
    "holding_rule": (
        "以『上月 prev_m = m-1』的 PB 橫切面，在『上月的 TopN 宇宙（pool[prev_m]）』內排序；"
        "（你在 export_returns.py 用 pe_low_signal 套到 pb_ratio 上）"
        "若 require_positive=True 則只保留 PB>0；"
        "取 PB 最低的 bottom_frac=30%；"
        "於『本月 m』整月等權持有"
    ),
    "params": {
        "bottom_frac": 0.30,
        "require_positive": True,
        "source_func": "pe_low_signal (applied to pb_ratio)",
        "decision_month": "m-1",
        "holding_month": "m"
    },
    "timing_notes": "同 PE_low：上月指標決定本月持有；避免前視"
}

LOW_BETA_META = {
    "factor": "Low_beta",
    "display_name": "低 Beta（用 pe_low_signal 選最小值）",
    "category": "Defensive",
    "rebalance": "M",
    "universe": "Top200 (由 pool 決定)",
    "holding_rule": (
        "以『上月 prev_m = m-1』的 beta 橫切面，在『上月的 TopN 宇宙（pool[prev_m]）』內排序；"
        "require_positive=True 只保留 beta>0；"
        "取 beta 最低的 bottom_frac=30%；"
        "於『本月 m』整月等權持有"
    ),
    "params": {
        "bottom_frac": 0.30,
        "require_positive": True,
        "source_func": "pe_low_signal (applied to beta)",
        "decision_month": "m-1",
        "holding_month": "m"
    },
    "timing_notes": "以 beta 的上月值做排序決定本月持有；避免前視"
}

HIGH_YIELD_META = {
    "factor": "High_yield",
    "display_name": "高殖利率",
    "category": "Value",
    "rebalance": "M",
    "universe": "Top200 (由 pool 決定)",
    "holding_rule": (
        "以『上月 prev_m = m-1』的殖利率 DY 橫切面，在『上月的 TopN 宇宙（pool[prev_m]）』內排序；"
        "若 require_positive=True 則只保留 DY>0；"
        "取 DY 最高的 top_frac=30%；"
        "於『本月 m』整月等權持有"
    ),
    "params": {
        "top_frac": 0.30,
        "require_positive": False,  # 你 export_returns.py 呼叫 require_positive=False
        "source_func": "dy_high_signal",
        "decision_month": "m-1",
        "holding_month": "m"
    },
    "timing_notes": "dy_high_signal 用上月 DY 決定本月持有；避免前視"
}

HIGH_YOY_META = {
    "factor": "High_yoy",
    "display_name": "高盈餘年增率",
    "category": "Growth",
    "rebalance": "M",
    "universe": "Top200 (你修正為用 pool[m])",
    "holding_rule": (
        "以『prev_m = m-2』的 YoY 橫切面排序（yoy_is_percent=True 會先 /100）；"
        "並剔除超過 yoy_cap_ratio 的極端值（預設 200%）；"
        "在『本月 m 的 TopN 宇宙（pool[m]）』內取 YoY 最高的 top_frac=30%；"
        "於『本月 m』整月等權持有"
    ),
    "params": {
        "top_frac": 0.30,
        "yoy_cap_ratio": 200,
        "yoy_is_percent": True,
        "require_positive": False,
        "decision_month": "m-2 (YoY data month)",
        "holding_month": "m",
        "universe_key_used": "pool[m]",
        "source_func": "yoy_high_signal"
    },
    "timing_notes": "yoy_high_signal 目前用『兩個月前 prev_m=m-2 的 YoY』決定『本月 m 持有』；且宇宙使用 pool[m]（你註記為關鍵修正）"
}

EPS_GROWTH_META = {
    "factor": "EPS_growth",
    "display_name": "EPS 預估成長",
    "category": "Growth",
    "rebalance": "M",
    "universe": "Top200 (用 pool[m])",
    "holding_rule": (
        "針對持有月 m：取觀察月 t=m-1 與前一月 t-1=m-2 的 EPS 預估值比較；"
        "若 increase_strict=True 則要求 EPS[t] > EPS[t-1]（否則 EPS[t] >= EPS[t-1]）；"
        "若 require_positive=True 則要求兩期 EPS 均 > 0；"
        "符合者於『本月 m』整月等權持有"
    ),
    "params": {
        "increase_strict": True,
        "require_positive": True,
        "decision_months_used": ["m-1", "m-2"],
        "holding_month": "m",
        "universe_key_used": "pool[m]",
        "source_func": "eps_growth_signal"
    },
    "timing_notes": "eps_growth_signal 用 (m-1) 與 (m-2) 的 EPS 預估決定 m 月持有；避免前視需確保 eps_est 為當時可得的預估快照"
}

MARGIN_GROWTH_META = {
    "factor": "Margin_growth",
    "display_name": "利潤率連兩季成長（Gross & Operating）",
    "category": "Quality",
    "rebalance": "Q (但月內會套 mktcap_pool 做交集)",
    "universe": "Top200 ex-fin (每月再與 pool[m] 交集)",
    "holding_rule": (
        "將 gross 與 operating（你傳入的是 rev）先對齊到 Q-DEC 季別（以該季最後一筆公告代表）；"
        "判斷每季是否『連續兩季成長』（allow_equal=False 時為嚴格成長）；"
        "為避免前視，對成長判斷結果 shift(1)，代表進場用的是『上季已確定』的成長訊號；"
        "每季 q 對應一個進場月份（Q1→6月、Q2→9月、Q3→12月、Q4→次年4月），"
        "並用該進場月的『最後一個交易日』作為持有開始日；"
        "持有到下一次進場日前一日；持有期間每個月再與 pool[m] 取交集"
    ),
    "params": {
        "allow_equal": False,
        "source_func": "margin_growth_signal",
        "quarter_entry_rule": "Q1->Jun, Q2->Sep, Q3->Dec, Q4->Apr(next year)",
        "start_day_rule": "entry month last trading day",
        "anti_lookahead": "both_ok = (gm_ok & om_ok).shift(1)"
    },
    "timing_notes": "此因子是『季訊號 + 月宇宙過濾 + 日頻展開』，時間對齊最敏感；你已用 shift(1) 明確避免前視"
}

QUANTREND_META = {
    "factor": "QuanTrend",
    "display_name": "QuanTrend（價格趨勢 × EPS 趨勢 × 估值）",
    "category": "Multi-Factor",
    "rebalance": "M",
    "universe": "Top200 (用 pool[m])",
    "holding_rule": (
        "決策月 t=m-1："
        "（1）本月底 60MA > 上月底 60MA；"
        "（2）本月底 EPS 預估方向為上（EPS[t]>EPS[t-1] 或 EPS 持平但延續上期上升方向）；"
        "（3）在同時符合(1)(2)者中，以本月底 PE 由小到大取前 n_select 檔；"
        "於『下個月 m』整月等權持有"
    ),
    "params": {
        "ma_window": 60,
        "n_select": 20,
        "require_positive_pe": True,
        "decision_month": "t=m-1",
        "holding_month": "m",
        "universe_key_used": "pool[m]",
        "source_func": "quantrend_sig"
    },
    "timing_notes": "quantrend_sig 明確用『決策月 t』資訊決定『下月 m=t+1』持有；避免前視偏誤"
}

MARGIN_SURPRISE_META = {
    "factor": "Margin_surprise",
    "display_name": "營利率 Surprise Index（SI）",
    "category": "Quality",
    "rebalance": "Q (但月內會套 mktcap_pool 做交集)",
    "universe": "Top200 (每月再與 pool[m] 交集)",
    "holding_rule": (
        "將季頻營利率 margin_q 對齊至 Q-DEC；計算 YoY 變化 ΔMargin(q)=Margin(q)-Margin(q-4)；"
        "以公告月最後一個交易日近似公告日：Q1→5月、Q2→8月、Q3→11月、Q4→次年3月；"
        "計算 PR(q-1)：前一季公告後一日至本季公告前一日的股價報酬；"
        "計算 SI(q)=Z(ΔMargin(q)) - Z(PR(q-1))；"
        "取 SI>0 且排名前 top_frac=20% 做多；"
        "進場/持有區間沿用 margin_growth_signal 的季度進場月份規則（Q1→6月、Q2→9月、Q3→12月、Q4→次年4月），"
        "持有期間每個月再與 pool[m] 取交集"
    ),
    "params": {
        "top_frac": 0.20,
        "require_positive_margin": False,
        "source_func": "margin_surprise_signal",
        "announce_month_rule": "Q1->May, Q2->Aug, Q3->Nov, Q4->Mar(next year)",
        "entry_month_rule": "Q1->Jun, Q2->Sep, Q3->Dec, Q4->Apr(next year)"
    },
    "timing_notes": "此因子以公告日窗口建構 PR(q-1)；避免前視需確保公告月與交易日對齊合理（你用『公告月最後交易日』近似）"
}


# Convenience registry (you can delete if you don't need it)
FACTOR_META_REGISTRY = {
    "Top200": TOP200_META,
    "Momentum_01": MOMENTUM_01_META,
    "Momentum_03": MOMENTUM_03_META,
    "Momentum_06": MOMENTUM_06_META,
    "PE_low": PE_LOW_META,
    "PB_low": PB_LOW_META,
    "Low_beta": LOW_BETA_META,
    "High_yield": HIGH_YIELD_META,
    "High_yoy": HIGH_YOY_META,
    "EPS_growth": EPS_GROWTH_META,
    "Margin_growth": MARGIN_GROWTH_META,
    "QuanTrend": QUANTREND_META,
    "Margin_surprise": MARGIN_SURPRISE_META,
}




#=========================







def build_sample_pool(mktcap: pd.DataFrame, top_n: int = 200) -> dict:
    pool = {}
    for ym, row in mktcap.iterrows():
        # 當月計算出來的市值 -> 用在下個月
        period = pd.Period(ym, freq="M") + 1
        top_stocks = row.dropna().nlargest(top_n).index
        pool[period] = set(top_stocks)
    return pool
def build_sample_pool_ex_fin(mktcap: pd.DataFrame, fin_df: pd.DataFrame, top_n: int = 200) -> dict[pd.Period, set]:
    """
    以「當月市值」決定「下個月」的 Top-N 宇宙（排除金融股）：
    pool[當月 + 1] = 當月TopN (去掉金融股)。
    """
    # 取金融股代碼 set
    financial_stocks = set(fin_df.iloc[:, 0].astype(str).str.strip())

    mc = mktcap.copy()
    mc.columns = mc.columns.astype(str).str.strip()
    if not isinstance(mc.index, pd.PeriodIndex):
        mc.index = pd.to_datetime(mc.index).to_period("M")

    pool: dict[pd.Period, set] = {}
    for ym, row in mc.iterrows():
        topn = set(row.dropna().nlargest(top_n).index)
        # 去掉金融股
        filtered = topn - financial_stocks
        pool[ym + 1] = filtered
    return pool


def momentum_signal(returns: pd.DataFrame,
                    mktcap_pool: dict,
                    top_frac: float = 0.30,
                    lookback_months: int = 1) -> pd.DataFrame:
    """
    動能訊號（可調回看月數，預設=1 等於原本的「當月MTD」）：
      1) 以當月 m 的 Top200 宇宙做篩選
      2) 在該宇宙內，用過去 lookback_months 個月份（含 m）的日報酬做幾何累積：∏(1+r)-1
      3) 先取全體中的前 top_frac，再從其中保留 > 0
      4) 配置到下一個月 (m+1) 的所有交易日
    回傳：與 returns 同尺寸的 0/1 DataFrame
    """
    r = returns.sort_index()
    signal = pd.DataFrame(0, index=r.index, columns=r.columns, dtype="int8")
    month_key = r.index.to_period("M")

    for m, _ in r.groupby(month_key):
        # 1) 當月宇宙
        universe = list(r.columns.intersection(mktcap_pool.get(m, set())))
        if not universe:
            continue

        # 2) 回看期（含當月）：m - (L-1) ... m
        months = [(m - i) for i in range(lookback_months - 1, -1, -1)]
        win_mask = month_key.isin(months)
        r_win = r.loc[win_mask, universe]

        # 3) 幾何累積報酬（若整段缺值則為 NaN）
        mom = (1.0 + r_win).prod(min_count=1) - 1.0
        mom = mom.dropna()
        if mom.empty:
            continue

        # 4) 先取前 top_frac，再濾 > 0
        k = max(1, int(np.ceil(len(mom) * top_frac)))
        topk = mom.nlargest(k)
        winners = topk[topk > 0].index
        if len(winners) == 0:
            continue

        # 5) 配置到下一個月
        next_mask = (month_key == (m + 1))
        if next_mask.any():
            signal.loc[next_mask, winners] = 1

    signal.index.name = r.index.name
    signal.columns.name = r.columns.name
    return signal


import pandas as pd

def pool_to_alpha(returns: pd.DataFrame, pool: dict) -> pd.DataFrame:
    """
    把 monthly pool (dict: Period -> set of tickers)
    轉換成日頻 alpha 矩陣 (0/1)，大小與 returns 相同。
    
    - returns: DataFrame, index=日 (DatetimeIndex), columns=股票代號
    - pool: dict, key=Period('YYYY-MM','M'), value=set(股票代號)
    """
    r = returns.sort_index()
    signal = pd.DataFrame(0, index=r.index, columns=r.columns, dtype="int8")

    month_key = r.index.to_period("M")

    for m, r_m in r.groupby(month_key):
        if m not in pool:
            continue

        # 取這個月的樣本池
        sample = list(r_m.columns.intersection(pool[m]))

        # 標記到「下一個月」的所有交易日
        next_mask = (month_key == (m + 1))
        if next_mask.any():
            signal.loc[next_mask, sample] = 1

    return signal

import pandas as pd
import numpy as np

def eps_growth_signal(
    returns: pd.DataFrame,
    eps_est: pd.DataFrame,                 # 預估 EPS（月頻）
    mktcap_pool: dict[pd.Period, set],     # 來自 build_sample_pool（key=Period('YYYY-MM','M')）
    increase_strict: bool = True,          # True: EPS[t] >  EPS[t-1]；False: EPS[t] >= EPS[t-1]
    require_positive: bool = False,        # True: 僅在 EPS[t], EPS[t-1] 皆 > 0 時才納入
) -> pd.DataFrame:
    """
    規則：比較 t 與 t-1 月的預估 EPS，若有成長，則在 t+1 月把該股票納入持有。
    回傳：與 returns 同 shape 的 0/1 訊號（int8）
    """
    # ---- 基礎清洗 ----
    r = returns.sort_index().copy()
    assert isinstance(r.index, pd.DatetimeIndex), "returns.index 必須是 DatetimeIndex（日頻）"
    r.columns = r.columns.astype(str).str.strip()

    eps = eps_est.copy()
    eps.columns = eps.columns.astype(str).str.strip()
    if not isinstance(eps.index, pd.PeriodIndex):
        eps.index = pd.to_datetime(eps.index).to_period("M")

    signal = pd.DataFrame(0, index=r.index, columns=r.columns, dtype="int8")
    month_key = r.index.to_period("M")

    # ---- 主迴圈（逐月持有）----
    for m in month_key.unique():
        # 要決定「本月 m 的持有」，需用 (m-1) 與 (m-2) 的 EPS 來判斷
        t     = m -1  # 當作「觀察月」
        t_1   = m - 2   # 當作「前一月」

        # 宇宙採用 pool[m]（對齊「下月持有 = 由上月市值決定的下月池」的邏輯）
        universe = pd.Index(sorted(mktcap_pool.get(m, set()))).astype(str).str.strip()
        universe = r.columns.intersection(universe)

        if universe.empty or (t not in eps.index) or (t_1 not in eps.index):
            continue

        e_t   = pd.to_numeric(eps.loc[t,   universe], errors="coerce")
        e_t1  = pd.to_numeric(eps.loc[t_1, universe], errors="coerce")

        # 僅保留同時非空的橫切面
        valid = (~e_t.isna()) & (~e_t1.isna())
        if not valid.any():
            continue

        e_t  = e_t[valid]
        e_t1 = e_t1[valid]

        # （可選）要求兩期 EPS 皆為正
        if require_positive:
            pos = (e_t > 0) & (e_t1 > 0)
            if not pos.any():
                continue
            e_t  = e_t[pos]
            e_t1 = e_t1[pos]

        # 成長條件
        if increase_strict:
            picks = (e_t >  e_t1)
        else:
            picks = (e_t >= e_t1)

        picks = e_t.index[picks]
        if len(picks) == 0:
            continue

        # 在「本月 m 的所有交易日」標 1
        hold_mask = (month_key == m)
        signal.loc[hold_mask, picks] = 1

    signal.index.name = r.index.name
    signal.columns.name = r.columns.name
    return signal

import pandas as pd
import numpy as np

def build_sample_pool(mktcap: pd.DataFrame, top_n: int = 200) -> dict[pd.Period, set]:
    """
    以「當月市值」決定「下個月」的可投資池（Top-N）。
    mktcap: 月頻，index 可為每月任意日（建議月底），columns=股票代碼
    回傳：{Period('YYYY-MM','M') -> set(TopN tickers)}
    """
    # 1) 統一欄名為字串、去空白
    mktcap = mktcap.copy()
    mktcap.columns = mktcap.columns.astype(str).str.strip()

    # 2) 確保索引是月 PeriodIndex
    if not isinstance(mktcap.index, pd.PeriodIndex):
        mktcap.index = pd.to_datetime(mktcap.index).to_period("M")

    pool: dict[pd.Period, set] = {}
    for ym, row in mktcap.iterrows():
        nxt = ym + 1  # 當月市值 -> 下月可投資池
        top_stocks = row.dropna().nlargest(top_n)
        pool[nxt] = set(top_stocks.index)
    return pool


def pe_low_signal(
    returns: pd.DataFrame,
    pe_ratio: pd.DataFrame,
    mktcap_pool: dict[pd.Period, set],
    bottom_frac: float = 0.30,
    require_positive: bool = True,
) -> pd.DataFrame:
    """
    以「上個月 PE」在 TopN 宇宙中挑選最低本益比的 bottom_frac 標的，整個「本月」持有。
    returns : 日頻，index=交易日(DatetimeIndex)，columns=股票代碼
    pe_ratio: 月頻，index=月(Period/Timestamp皆可)、columns=股票代碼，值=PE
    mktcap_pool : {Period('YYYY-MM','M') -> set(tickers)}，通常來自 build_sample_pool
    回傳：0/1 訊號（int8）
    """
    # ---- 基礎清洗與對齊 ----
    r = returns.sort_index()
    assert isinstance(r.index, pd.DatetimeIndex), "returns.index 必須是 DatetimeIndex（日頻）"
    r_cols = r.columns.astype(str).str.strip()

    pe = pe_ratio.copy()
    pe.columns = pe.columns.astype(str).str.strip()
    if not isinstance(pe.index, pd.PeriodIndex):
        pe.index = pd.to_datetime(pe.index).to_period("M")

    # 把 returns 欄名也標準化成字串
    r = r.copy()
    r.columns = r_cols

    # 建 0/1 訊號容器（省記憶體用 int8）
    signal = pd.DataFrame(0, index=r.index, columns=r.columns, dtype="int8")

    # 以月份分組持有（本月持有 = 上月PE 的結果）
    month_key = r.index.to_period("M")
    unique_months = month_key.unique()

    # ---- 主迴圈（逐月）----
    for m in unique_months:
        prev_m = m - 1  # 依規則，上月為決策月

        # 宇宙：上月的 TopN；與 returns 欄交集以避免 KeyError
        universe = pd.Index(sorted(mktcap_pool.get(prev_m, set()))).astype(str).str.strip()
        universe = r.columns.intersection(universe)
        if universe.empty:
            continue

        # 上月 PE 的橫切面（只取宇宙的欄）
        if prev_m not in pe.index:
            continue
        pe_prev = pd.to_numeric(pe.loc[prev_m, universe], errors="coerce").dropna()

        if require_positive:
            pe_prev = pe_prev[pe_prev > 0]

        if pe_prev.empty:
            continue

        # 取「最低 bottom_frac」的標的
        k = max(1, int(np.ceil(len(pe_prev) * bottom_frac)))
        picks = pe_prev.nsmallest(k).index  # 本月要持有的標的

        # 把這些標的在「本月所有交易日」標 1
        hold_mask = (month_key == m)
        if hold_mask.any():
            signal.loc[hold_mask, picks] = 1

    signal.index.name = r.index.name
    signal.columns.name = r.columns.name
    return signal

import pandas as pd
import numpy as np

# ---------------------------
# 1) 市值 Top-N（下月）投資池
# ---------------------------
def build_sample_pool(mktcap: pd.DataFrame, top_n: int = 200) -> dict[pd.Period, set]:
    """
    以「當月市值」決定「下個月」的 Top-N 宇宙：
    pool[當月 + 1] = 當月TopN。月度對齊、避免前視。
    """
    mc = mktcap.copy()
    mc.columns = mc.columns.astype(str).str.strip()
    if not isinstance(mc.index, pd.PeriodIndex):
        mc.index = pd.to_datetime(mc.index).to_period("M")

    pool: dict[pd.Period, set] = {}
    for ym, row in mc.iterrows():
        pool[ym + 1] = set(row.dropna().nlargest(top_n).index)
    return pool


# ---------------------------
# 2) 將「公告月份」→「所屬季(Q-DEC)」
# ---------------------------
def align_announce_to_quarter(df: pd.DataFrame) -> pd.DataFrame:
    """
    將公告月份對齊到 Q-DEC（會用該季最後一筆公告作為代表值）
    """
    x = df.copy()
    x.columns = x.columns.astype(str).str.strip()

    if isinstance(x.index, pd.PeriodIndex):
        ts = x.index.to_timestamp()
    else:
        ts = pd.to_datetime(x.index)

    labels = []
    for y, m in zip(ts.year, ts.month):
        if   m in (4, 5):   qy, qn = y,   1
        elif m in (7, 8):   qy, qn = y,   2
        elif m in (10, 11): qy, qn = y,   3
        elif m in (1, 2, 3):qy, qn = y-1, 4
        elif m == 6:        qy, qn = y,   2
        elif m == 9:        qy, qn = y,   3
        elif m == 12:       qy, qn = y,   4
        else:
            labels.append(pd.Period(f"{y}-{m:02d}", "M").asfreq("Q-DEC"))
            continue
        labels.append(pd.Period(f"{qy}Q{qn}", "Q-DEC"))

    qidx = pd.PeriodIndex(labels, freq="Q-DEC")
    return x.groupby(qidx).last()


# ---------------------------
# 3) 連兩季成長判斷
# ---------------------------
def two_consecutive_growth(df_q: pd.DataFrame) -> pd.DataFrame:
    """
    在季別 q 上為 True 的條件：
    df[q] > df[q-1] 且 df[q-1] > df[q-2]
    """
    z = df_q.apply(pd.to_numeric, errors="coerce")
    pos = z.diff().gt(0)
    ok2 = (pos & pos.shift(1)).fillna(False)
    return ok2


# ---------------------------
# 4) 季度 → 實際進場月份（公告截止後 → 下個月初持有）
# ---------------------------
def quarter_entry_month(q: pd.Period) -> pd.Period:
    y = int(q.year)
    if q.quarter == 1:   # Q1 公告 5/15，6 月初開始持有
        return pd.Period(f"{y}-06", "M")
    if q.quarter == 2:   # Q2 公告 8/14，9 月初開始持有
        return pd.Period(f"{y}-09", "M")
    if q.quarter == 3:   # Q3 公告 11/14，12 月初開始持有
        return pd.Period(f"{y}-12", "M")
    return pd.Period(f"{y+1}-04", "M")  # Q4 年報 → 次年 4 月初開始持有


# ---------------------------
# 5) 公告月份 → 該月最後一個交易日
# ---------------------------
def month_last_trading_day(month_period: pd.Period, trading_index: pd.DatetimeIndex) -> pd.Timestamp | None:
    mask = trading_index.to_period("M") == month_period
    if not mask.any():
        return None
    return trading_index[mask][-1]


# ---------------------------
# 6) 主函式：利潤率成長（日頻 0/1 訊號）
# ---------------------------
def margin_growth_signal(
    returns: pd.DataFrame,
    gross: pd.DataFrame,
    operating: pd.DataFrame,
    mktcap_pool: dict[pd.Period, set],
    allow_equal: bool = False,
) -> pd.DataFrame:
    # 1) 對齊 returns
    r = returns.sort_index()
    if not isinstance(r.index, pd.DatetimeIndex):
        raise ValueError("returns.index 必須是 DatetimeIndex（日頻）")
    cols = r.columns.astype(str).str.strip()
    r = r.copy()
    r.columns = cols

    # 2) 季化 + 連兩季成長布林表
    gm_q = align_announce_to_quarter(gross).reindex(columns=cols, copy=False)
    om_q = align_announce_to_quarter(operating).reindex(columns=cols, copy=False)

    if allow_equal:
        gm_ok = (gm_q.diff().ge(0) & gm_q.diff().ge(0).shift(1)).fillna(False)
        om_ok = (om_q.diff().ge(0) & om_q.diff().ge(0).shift(1)).fillna(False)
    else:
        gm_ok = two_consecutive_growth(gm_q)
        om_ok = two_consecutive_growth(om_q)

    # 🚨 修正：避免前視 → shift(1)，進場用的是「上季」的判斷結果
    both_ok = (gm_ok & om_ok).shift(1)

    # 3) 找每一季的「實際進場日」
    decision_tbl = []
    for q in both_ok.index:
        entry_m = quarter_entry_month(q)
        entry_dt = month_last_trading_day(entry_m, r.index)
        if entry_dt is None:
            continue
        decision_tbl.append((q, entry_dt))

    if not decision_tbl:
        return pd.DataFrame(0, index=r.index, columns=cols, dtype="int8")

    # 4) 建立訊號矩陣
    signal = pd.DataFrame(0, index=r.index, columns=cols, dtype="int8")

    for i, (q, start_dt) in enumerate(decision_tbl):
        sel = both_ok.loc[q]
        if sel is None or not sel.any():
            continue
        picks_idx = pd.Index(sel.index[sel.values])

        if i + 1 < len(decision_tbl):
            next_start = decision_tbl[i + 1][1]
            end_pos = r.index.get_indexer_for([next_start])[0] - 1
            if end_pos < 0:
                continue
            end_dt = r.index[end_pos]
        else:
            end_dt = r.index[-1]

        if end_dt < start_dt:
            continue

        date_slice = r.loc[start_dt:end_dt]
        slice_month = date_slice.index.to_period("M")

        for m in slice_month.unique():
            universe = pd.Index(sorted(mktcap_pool.get(m, set()))).astype(str).str.strip()
            uni_cols = signal.columns.intersection(universe)
            final = uni_cols.intersection(picks_idx)
            if final.empty:
                continue
            idx_in_slice = date_slice.index[slice_month == m]
            signal.loc[idx_in_slice, final] = 1

    signal.index.name = r.index.name
    signal.columns.name = r.columns.name
    return signal

import pandas as pd
import numpy as np

# ------------------------------------------------------------
# 產生 Top-N 市值「下月」投資池（和你原本的一樣，但做了型別/索引統一）
# ------------------------------------------------------------
def build_sample_pool(mktcap: pd.DataFrame, top_n: int = 200) -> dict[pd.Period, set]:
    """
    mktcap: 月頻 DataFrame，index 可為任意日期，columns=股票代碼，值=市值
    回傳: {Period('YYYY-MM','M') -> set(TopN tickers)}，代表「下個月」的投資池
    """
    mc = mktcap.copy()
    mc.columns = mc.columns.astype(str).str.strip()
    if not isinstance(mc.index, pd.PeriodIndex):
        mc.index = pd.to_datetime(mc.index).to_period("M")

    pool: dict[pd.Period, set] = {}
    for ym, row in mc.iterrows():
        pool[ym + 1] = set(row.dropna().nlargest(top_n).index)
    return pool


# ------------------------------------------------------------
# 殖利率高因子：上月 DY 在 Top200 宇宙內取「最高的 top_frac」
# 本月整月持有（訊號 0/1）
# ------------------------------------------------------------
def dy_high_signal(
    returns: pd.DataFrame,
    dy_ratio: pd.DataFrame,
    mktcap_pool: dict[pd.Period, set],
    top_frac: float = 0.30,
    require_positive: bool = True,
) -> pd.DataFrame:
    """
    returns : 日頻 DataFrame，index=交易日(DatetimeIndex)，columns=股票代碼
    dy_ratio: 月頻 DataFrame，index=月(Period/Timestamp 皆可)，columns=股票代碼，值=殖利率
              （通常是「該月月底」對應的殖利率）
    mktcap_pool : {Period('YYYY-MM','M') -> set(Top200 tickers)}，來自 build_sample_pool
    top_frac : 取殖利率最高前 x%
    require_positive : 是否只保留 DY > 0（多數情況建議 True）

    回傳：與 returns 同 shape 的 0/1 訊號（int8）
    """
    # 基礎清洗
    r = returns.sort_index().copy()
    assert isinstance(r.index, pd.DatetimeIndex), "returns.index 需為 DatetimeIndex（日頻）"
    r.columns = r.columns.astype(str).str.strip()

    dy = dy_ratio.copy()
    dy.columns = dy.columns.astype(str).str.strip()
    if not isinstance(dy.index, pd.PeriodIndex):
        dy.index = pd.to_datetime(dy.index).to_period("M")

    signal = pd.DataFrame(0, index=r.index, columns=r.columns, dtype="int8")

    # 以月份分組：本月持有 = 由「上月」DY 決定
    month_key = r.index.to_period("M")

    for m in month_key.unique():
        prev_m = m - 1  # 決策月
        # 上月的 Top200 宇宙，和 returns 欄位取交集避免 KeyError
        universe = pd.Index(sorted(mktcap_pool.get(prev_m, set()))).astype(str).str.strip()
        universe = r.columns.intersection(universe)
        if universe.empty or (prev_m not in dy.index):
            continue

        # 取上月 DY 橫切面（只取宇宙），轉數字、剔除 NA
        dy_prev = pd.to_numeric(dy.loc[prev_m, universe], errors="coerce").dropna()
        if require_positive:
            dy_prev = dy_prev[dy_prev > 0]

        if dy_prev.empty:
            continue

        # 取殖利率「最高」的前 top_frac
        k = max(1, int(np.ceil(len(dy_prev) * top_frac)))
        picks = dy_prev.nlargest(k).index  # 注意：和 PE 取最小不同，這裡取最大

        # 本月所有交易日標 1
        mask = (month_key == m)
        if mask.any():
            signal.loc[mask, picks] = 1

    signal.index.name = r.index.name
    signal.columns.name = r.columns.name
    return signal




import pandas as pd
import numpy as np


def yoy_high_signal(
    returns: pd.DataFrame,
    yoy_ratio: pd.DataFrame,
    mktcap_pool: dict[pd.Period, set],
    top_frac: float = 0.30,
    yoy_cap_ratio: float = 200,     # 你的 YoY 是百分比口徑
    yoy_is_percent: bool = True,    # ← 你的數據是百分比（如 248.84）
    require_positive: bool = False, # 依你條件：不強制 >0
) -> pd.DataFrame:
    r = returns.sort_index().copy()
    r.columns = r.columns.astype(str).str.strip()
    assert isinstance(r.index, pd.DatetimeIndex)

    yoy = yoy_ratio.copy()
    yoy.columns = yoy.columns.astype(str).str.strip()
    if not isinstance(yoy.index, pd.PeriodIndex):
        yoy.index = pd.to_datetime(yoy.index).to_period("M")

    signal = pd.DataFrame(0, index=r.index, columns=r.columns, dtype="int8")
    month_key = r.index.to_period("M")

    for m in month_key.unique():
        prev_m = m - 1

        # --- 這一行是關鍵修正：本月 m 的宇宙該用 pool[m] ---
        universe = pd.Index(sorted(mktcap_pool.get(m, set()))).astype(str).str.strip()  # ← 修正
        universe = r.columns.intersection(universe)
        if universe.empty or (prev_m not in yoy.index):
            continue

        yoy_prev = pd.to_numeric(yoy.loc[prev_m, universe], errors="coerce")
        yoy_prev = yoy_prev.replace([np.inf, -np.inf], np.nan).dropna()

        # 百分比→比率（若 yoy_is_percent=True）
        cap = yoy_cap_ratio
        if yoy_is_percent:
            yoy_prev = yoy_prev / 100.0
            cap = cap / 100.0

        if require_positive:
            yoy_prev = yoy_prev[yoy_prev > 0]
        yoy_prev = yoy_prev[yoy_prev <= cap]

        if yoy_prev.empty:
            continue

        k = max(1, int(np.ceil(len(yoy_prev) * top_frac)))
        picks = yoy_prev.nlargest(k).index

        signal.loc[month_key == m, picks] = 1

    signal.index.name = r.index.name
    signal.columns.name = r.columns.name
    return signal



def _compute_eps_direction(eps: pd.DataFrame) -> pd.DataFrame:
    """
    對每檔個股計算 EPS 變動方向（用於條件二的「延續上月變動方向」）：
      dir[t] =  1  若 EPS[t] >  EPS[t-1]
      dir[t] = -1  若 EPS[t] <  EPS[t-1]
      dir[t] = dir[t-1] 若 EPS[t] == EPS[t-1]（延續前一期變動方向）
      dir[t] =  0  其他情況（含前期為 NaN）
    回傳: 與 eps 同 index/columns 的 int8 DataFrame
    """
    z = eps.apply(pd.to_numeric, errors="coerce")
    out = pd.DataFrame(0, index=z.index, columns=z.columns, dtype="int8")

    for col in z.columns:
        s = z[col].values
        d = np.zeros(len(s), dtype="int8")
        prev_dir = 0
        prev_val = np.nan

        for i, val in enumerate(s):
            if np.isnan(val) or np.isnan(prev_val):
                # 一開始或前一期缺值，無法判斷方向
                d[i] = 0
            else:
                if val > prev_val:
                    d[i] = 1
                elif val < prev_val:
                    d[i] = -1
                else:
                    # val == prev_val → 延續前一期方向
                    d[i] = prev_dir
            prev_val = val
            prev_dir = d[i]

        out[col] = d

    return out


def quantrend_sig(
    returns: pd.DataFrame,
    prices: pd.DataFrame,                   # 日頻股價（收盤價）
    eps_est: pd.DataFrame,                  # 月頻「EPS 預估值」
    pe_ratio: pd.DataFrame,                 # 月頻「本益比」
    mktcap_pool: dict[pd.Period, set],      # 來自 build_sample_pool，key=Period('YYYY-MM','M')
    n_select: int = 20,                     # 每月選取檔數（依 PE 由低到高）
    require_positive_pe: bool = True,       # 是否要求 PE > 0
    ma_window: int = 60,                    # 60 日均價視窗
) -> pd.DataFrame:
    """
    新因子選股條件（避免前視，持有在「下個月」）：

    條件一：本月底60日均價 > 上月底60日均價
    條件二：本月底EPS預估值 > 上月底EPS預估值；
           若本月底EPS = 上月底EPS，則延續上一期EPS變動方向（向上才算符合）
    條件三：在同時符合條件一與條件二之個股中，
           依「本月底」本益比由小到大排序，選本益比最低之 n_select 檔個股

    時間對齊（無前視）：
      - 用「決策月 t」的資訊（t 月底 60MA、t 月底 EPS & EPS 方向、t 月底 PE）
      - 來決定「下個月 m = t+1」整月的持有組合

    Input:
      returns : 日頻報酬，index = 交易日(DatetimeIndex)，columns = 股票代碼
      prices  : 日頻股價（通常為收盤價），index 與 returns 對齊/可對齊
      eps_est : 月頻 EPS 預估值，index = 月（Period 或 Timestamp）
      pe_ratio: 月頻本益比
      mktcap_pool: {Period('YYYY-MM','M') -> set(tickers)}，下月投資池（市值 Top-N）

    Output:
      signal : 與 returns 同 shape 的 0/1 訊號（int8）
    """
    # --------- 0) 基礎清洗與對齊 ---------
    r = returns.sort_index().copy()
    assert isinstance(r.index, pd.DatetimeIndex), "returns.index 必須是 DatetimeIndex（日頻）"
    r.columns = r.columns.astype(str).str.strip()

    # ---- 股價：日頻 → 60 日均價 → 月底 60MA（PeriodIndex, freq='M'）----
    p = prices.sort_index().copy()
    assert isinstance(p.index, pd.DatetimeIndex), "prices.index 必須是 DatetimeIndex（日頻）"
    p.columns = p.columns.astype(str).str.strip()

    # 只保留與 returns 重疊的欄位，避免多出不在 returns 的股票
    common_cols = r.columns.intersection(p.columns)
    p = p[common_cols]
    r = r[common_cols]

    # 計算 60 日移動平均（以日頻計算）
    ma = p.rolling(window=ma_window, min_periods=ma_window).mean()

    # 取「每月底」的 60MA：按月份 groupby，取每月最後一個交易日的 60MA
    month_idx = ma.index.to_period("M")
    ma60_m = ma.groupby(month_idx).last()
    # ma60_m 的 index 是 PeriodIndex("M")

    # ---- 月頻資料：EPS / PE → PeriodIndex("M") + 對齊欄位 ----
    def _to_monthly(df: pd.DataFrame) -> pd.DataFrame:
        x = df.copy()
        x.columns = x.columns.astype(str).str.strip()
        if not isinstance(x.index, pd.PeriodIndex):
            x.index = pd.to_datetime(x.index).to_period("M")
        # 只留與 returns 共同的欄位
        return x.reindex(columns=r.columns, copy=False)

    eps_m = _to_monthly(eps_est)
    pe_m  = _to_monthly(pe_ratio)

    # EPS 變動方向（條件二用）
    eps_dir = _compute_eps_direction(eps_m)

    # --------- 1) 建立結果訊號 ---------
    signal = pd.DataFrame(0, index=r.index, columns=r.columns, dtype="int8")
    month_key = r.index.to_period("M")

    # --------- 2) 逐月產生持有組合（持有月 m）---------
    for m in month_key.unique():
        t   = m - 1   # 決策月
        t_1 = m - 2   # 比較用的上一個月（for 60MA & EPS[t-1]）

        # 宇宙：下個月 m 的市值 Top-N 池
        universe = pd.Index(sorted(mktcap_pool.get(m, set()))).astype(str).str.strip()
        universe = r.columns.intersection(universe)
        if universe.empty:
            continue

        # 需要足夠的月頻資料
        if (t not in ma60_m.index) or (t_1 not in ma60_m.index):
            continue
        if (t not in eps_m.index) or (t_1 not in eps_m.index) or (t not in eps_dir.index):
            continue
        if t not in pe_m.index:
            continue

        # --------- 條件一：60 日均價成長（t vs t-1）---------
        ma_t  = pd.to_numeric(ma60_m.loc[t,   universe], errors="coerce")
        ma_t1 = pd.to_numeric(ma60_m.loc[t_1, universe], errors="coerce")
        valid_ma = (~ma_t.isna()) & (~ma_t1.isna())
        cond1 = (ma_t > ma_t1) & valid_ma
        if not cond1.any():
            continue

        # --------- 條件二：EPS 成長 + 持平延續方向 ---------
        # 使用 eps_dir[t] == 1 代表「往上」方向（含持平但此前向上）
        dir_t = eps_dir.loc[t, universe]
        cond2 = (dir_t == 1)

        cond12 = cond1 & cond2
        if not cond12.any():
            continue

        candidates = cond12[cond12].index  # 同時符合條件一 & 二的股票

        # --------- 條件三：在 candidates 中依 t 月底 PE 由低到高取 n_select 檔 ---------
        pe_t = pd.to_numeric(pe_m.loc[t, candidates], errors="coerce")
        pe_t = pe_t.replace([np.inf, -np.inf], np.nan).dropna()
        if require_positive_pe:
            pe_t = pe_t[pe_t > 0]

        if pe_t.empty:
            continue

        k = min(n_select, len(pe_t))
        picks = pe_t.nsmallest(k).index  # 下個月 m 要持有的名單

        # --------- 3) 在「持有月 m 的所有交易日」標記 1 ---------
        hold_mask = (month_key == m)
        if hold_mask.any():
            signal.loc[hold_mask, picks] = 1

    signal.index.name = r.index.name
    signal.columns.name = r.columns.name
    return signal








def quarter_announce_month(q: pd.Period) -> pd.Period:
    """
    近似「財報公告月份」：
      announce_month(q) = quarter_entry_month(q) - 1 個月
      Q1 → 5 月、Q2 → 8 月、Q3 → 11 月、Q4 → 次年 3 月
    """
    em = quarter_entry_month(q)
    return em - 1  # 月 Period


# ------------------------------------------------------------
# 2) 共用小工具
# ------------------------------------------------------------
def _cross_sectional_zscore(s: pd.Series) -> pd.Series:
    """
    橫切面 z-score：同一季、多檔股票標準化。
    """
    v = pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan)
    m = v.mean()
    std = v.std(ddof=0)
    if np.isnan(std) or std == 0:
        return pd.Series(index=s.index, dtype="float64")
    z = (v - m) / std
    return z.reindex(s.index)

import pandas as pd
import numpy as np

# ------------------------------------------------------------
# 工具：把「營利率資料」對齊到 Q-DEC 季別
#   支援：
#   1) index 是 PeriodIndex（freq='Q*' or 'M'）
#   2) index 是 TEJ 風格：202001, 202002, 202003, 202004（int 或字串）
# ------------------------------------------------------------
def _align_margin_to_quarter(margin_q: pd.DataFrame, cols: pd.Index) -> pd.DataFrame:
    """
    將營利率資料對齊到 Q-DEC 季別：
      - 若 index 已是季 PeriodIndex，轉為 Q-DEC
      - 若是月 PeriodIndex，先轉 timestamp 再轉 Q-DEC
      - 若是 TEJ 風格 202001/202002（年 + 季），解析成年、季 → Q-DEC
      - 其他情況一律用 pd.to_datetime 再轉 Q-DEC

    cols : 回傳時只保留與 returns 共同的欄位
    """
    x = margin_q.copy()
    x.columns = x.columns.astype(str).str.strip()
    idx = x.index

    # ---- case 1: index 已經是 PeriodIndex ----
    if isinstance(idx, pd.PeriodIndex):
        # 若本來就是季頻（Q-DEC 或其他），直接轉 Q-DEC
        if idx.freqstr is not None and idx.freqstr.upper().startswith("Q"):
            qidx = idx.asfreq("Q-DEC")
        else:
            # 月頻或其他 → 先轉成 timestamp，再轉 Q-DEC
            ts = idx.to_timestamp()
            qidx = ts.to_period("Q-DEC")
        x.index = qidx
        x = x.groupby(x.index).last()

    else:
        # ---- case 2: 非 PeriodIndex，處理 TEJ 風格 202001/202002/202003/202004 ----
        idx_str = pd.Index(idx).astype(str)

        # 檢查是否像 '202001' 這樣：前四碼是年份，後面的 1~4 代表季
        looks_like_yq = (
            idx_str.str.len().between(5, 6).all() and
            idx_str.str[:4].str.isnumeric().all()
        )

        if looks_like_yq:
            years = idx_str.str[:4].astype(int)
            qcode = idx_str.str[4:].astype(int)  # 1,2,3,4

            q_list = []
            for y, qn in zip(years, qcode):
                # 一般情況：1~4 直接視為第幾季
                if qn in (1, 2, 3, 4):
                    q_list.append(pd.Period(f"{y}Q{qn}", "Q-DEC"))
                else:
                    # 萬一有奇怪的值，就 fallback 用月份分組方式
                    if qn in (1, 2, 3):
                        qq = 1
                    elif qn in (4, 5, 6):
                        qq = 2
                    elif qn in (7, 8, 9):
                        qq = 3
                    else:
                        qq = 4
                    q_list.append(pd.Period(f"{y}Q{qq}", "Q-DEC"))

            qidx = pd.PeriodIndex(q_list, freq="Q-DEC")
            x.index = qidx
            x = x.groupby(x.index).last()

        else:
            # ---- case 3: 一般日期 index → 直接轉 datetime → Q-DEC ----
            ts = pd.to_datetime(idx)
            qidx = ts.to_period("Q-DEC")
            x.index = qidx
            x = x.groupby(x.index).last()

    x = x.reindex(columns=cols, copy=False)
    x = x.sort_index()
    return x


# ------------------------------------------------------------
# 營利率 Surprise Index 因子（跟 margin_growth_signal 同一套季更新架構）
# ------------------------------------------------------------
def margin_surprise_signal(
    returns: pd.DataFrame,
    prices: pd.DataFrame,                # 日頻收盤價
    margin_q: pd.DataFrame,              # 季頻營利率：index 用 202001/202002 表示 1~4 季也可以
    mktcap_pool: dict[pd.Period, set],   # 來自 build_sample_pool：{Period('YYYY-MM') -> set(tickers)}
    top_frac: float = 0.20,              # 每季取 Surprise Index 最前 x% 做多
    require_positive_margin: bool = False,
) -> pd.DataFrame:
    """
    營利率 Surprise Index 選股規則（避免前視）：

    定義（季 q）：
      1) ΔMargin^{YoY}_{i,q} = Margin_{i,q} - Margin_{i,q-4}
      2) PR_{i,q-1} = 前一季財報公告日後一日至本季公告日前一日的股價報酬
      3) SI_{i,q}   = Z(ΔMargin^{YoY}_{i,q}) - Z(PR_{i,q-1})

    策略：
      - 對每一季 q 算出 SI_{i,q}，取前 top_frac（且 SI>0）
      - 持有期間：沿用你 margin_growth_signal 的「季進場月 → 下一季進場月前一日」邏輯
      - 持有期間每個月再與 mktcap_pool[m] 取交集
    """
    # ---- 0) 基礎清洗與對齊 ----
    r = returns.sort_index().copy()
    assert isinstance(r.index, pd.DatetimeIndex), "returns.index 必須是 DatetimeIndex（日頻）"
    r.columns = r.columns.astype(str).str.strip()

    px = prices.sort_index().copy()
    assert isinstance(px.index, pd.DatetimeIndex), "prices.index 必須是 DatetimeIndex（日頻）"
    px.columns = px.columns.astype(str).str.strip()

    common_cols = r.columns.intersection(px.columns)
    r = r[common_cols]
    px = px[common_cols]

    # 營利率 → Q-DEC 季別對齊（支援 202001/202002 這種 index）
    margin_q_aligned = _align_margin_to_quarter(margin_q, common_cols)

    # YoY 變化：q vs q-4
    margin_yoy = margin_q_aligned - margin_q_aligned.shift(4)

    trading_index = px.index

    # ---- 1) 建立每季「公告日」近似值（用公告月最後一個交易日代表）----
    announce_dates: dict[pd.Period, pd.Timestamp] = {}
    for q in margin_q_aligned.index:
        ann_month = quarter_announce_month(q)  # 你前面已經定義過：Q1→5月、Q2→8月、Q3→11月、Q4→次年3月
        ann_dt = month_last_trading_day(ann_month, trading_index)
        if ann_dt is not None:
            announce_dates[q] = ann_dt

    if not announce_dates:
        return pd.DataFrame(0, index=r.index, columns=r.columns, dtype="int8")

    decision_quarters = sorted(announce_dates.keys(), key=lambda qq: announce_dates[qq])

    # ---- 2) 計算每季 q 的 PR_{q-1}（財報前一季股價先行反應）----
    pr_qm1 = pd.DataFrame(index=margin_q_aligned.index, columns=common_cols, dtype="float64")

    for q in decision_quarters:
        prev_q = q - 1
        if prev_q not in announce_dates:
            continue
        ann_prev = announce_dates[prev_q]
        ann_curr = announce_dates[q]

        start_pos = trading_index.searchsorted(ann_prev, side="right")
        if start_pos >= len(trading_index):
            continue
        end_pos = trading_index.searchsorted(ann_curr, side="left") - 1
        if end_pos <= start_pos:
            continue

        win_idx = trading_index[start_pos:end_pos + 1]
        px_win = px.loc[win_idx]

        px_ffill = px_win.ffill().bfill()
        first = px_ffill.iloc[0]
        last = px_ffill.iloc[-1]
        ret = (last - first) / first
        pr_qm1.loc[q, :] = ret.reindex(common_cols)

    # ---- 3) 每季 q 的 Surprise Index：Z(ΔMargin^{YoY}) - Z(PR_{q-1}) ----
    surprise_margin = pd.DataFrame(index=margin_q_aligned.index, columns=common_cols, dtype="float64")

    for q in decision_quarters:
        if q not in margin_yoy.index:
            continue
        delta = margin_yoy.loc[q]
        pr = pr_qm1.loc[q]

        if require_positive_margin and q in margin_q_aligned.index:
            m_t = pd.to_numeric(margin_q_aligned.loc[q], errors="coerce")
            pos_mask = (m_t > 0)
            delta = delta.where(pos_mask)
            pr = pr.where(pos_mask)

        z_delta = _cross_sectional_zscore(delta)
        z_pr = _cross_sectional_zscore(pr)

        si = z_delta - z_pr
        surprise_margin.loc[q] = si

    # ---- 4) 依 Surprise Index 橫切面排序，取前 top_frac（且 SI>0）做多 ----
    picks_by_quarter: dict[pd.Period, pd.Index] = {}

    for q in decision_quarters:
        si_q = pd.to_numeric(surprise_margin.loc[q], errors="coerce")
        si_q = si_q.replace([np.inf, -np.inf], np.nan).dropna()
        if si_q.empty:
            continue

        si_q = si_q[si_q > 0]
        if si_q.empty:
            continue

        k = max(1, int(np.ceil(len(si_q) * top_frac)))
        top_idx = si_q.nlargest(k).index
        picks_by_quarter[q] = top_idx

    # ---- 5) 把季別持股展開成日頻 0/1 訊號（調倉時間與 margin_growth_signal 一致）----
    signal = pd.DataFrame(0, index=r.index, columns=r.columns, dtype="int8")

    # decision_tbl: (q, start_dt)，start_dt = 進場月份最後一個交易日
    decision_tbl = []
    for q in decision_quarters:
        if q not in picks_by_quarter:
            continue
        entry_m = quarter_entry_month(q)
        start_dt = month_last_trading_day(entry_m, r.index)
        if start_dt is None:
            continue
        decision_tbl.append((q, start_dt))

    decision_tbl = sorted(decision_tbl, key=lambda x: x[1])

    if not decision_tbl:
        signal.index.name = r.index.name
        signal.columns.name = r.columns.name
        return signal

    for i, (q, start_dt) in enumerate(decision_tbl):
        picks = pd.Index(picks_by_quarter[q])
        if picks.empty:
            continue

        if i + 1 < len(decision_tbl):
            next_start = decision_tbl[i + 1][1]
            end_pos = r.index.searchsorted(next_start, side="left") - 1
            if end_pos < 0:
                continue
            end_dt = r.index[end_pos]
        else:
            end_dt = r.index[-1]

        if end_dt < start_dt:
            continue

        date_slice = r.loc[start_dt:end_dt]
        slice_month = date_slice.index.to_period("M")

        for m in slice_month.unique():
            universe = pd.Index(sorted(mktcap_pool.get(m, set()))).astype(str).str.strip()
            uni_cols = signal.columns.intersection(universe)
            final = uni_cols.intersection(picks)
            if final.empty:
                continue
            idx_in_slice = date_slice.index[slice_month == m]
            signal.loc[idx_in_slice, final] = 1

    signal.index.name = r.index.name
    signal.columns.name = r.columns.name
    return signal
