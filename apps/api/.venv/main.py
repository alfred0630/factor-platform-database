from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import json
import pandas as pd
import numpy as np

app = FastAPI(title="Factor Platform API", version="0.2.0")

# 允許前端跨域呼叫（開發期先放寬）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 專案根目錄 factor-platform/
APP_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = APP_ROOT / "data"

def load_factor_return(factor: str) -> pd.Series | None:
    fp = DATA_DIR / "returns" / f"{factor}.json"
    if not fp.exists():
        return None
    obj = json.loads(fp.read_text(encoding="utf-8"))
    s = pd.Series(obj["ret"], index=pd.to_datetime(obj["dates"]), name=factor).sort_index()
    return s

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/factors")
def list_factors():
    folder = DATA_DIR / "returns"
    if not folder.exists():
        return {"factors": []}
    factors = sorted([p.stem for p in folder.glob("*.json")])
    return {"factors": factors}

@app.get("/returns")
def get_returns(
    factor: str = Query(...),
    start: str | None = None,
    end: str | None = None,
):
    s = load_factor_return(factor)
    if s is None:
        return {"factor": factor, "dates": [], "ret": []}

    if start:
        s = s.loc[pd.to_datetime(start):]
    if end:
        s = s.loc[:pd.to_datetime(end)]

    return {
        "factor": factor,
        "dates": s.index.strftime("%Y-%m-%d").tolist(),
        "ret": s.values.tolist()
    }

@app.post("/metrics")
def metrics(payload: dict):
    """
    payload = {"factors":["PE",...], "start":"2025-01-01", "end":"2025-12-31", "rf":0.0, "freq":252}
    """
    factors = payload.get("factors", [])
    start = payload.get("start")
    end = payload.get("end")
    rf = float(payload.get("rf", 0.0))
    freq = int(payload.get("freq", 252))

    rows = []
    for f in factors:
        s = load_factor_return(f)
        if s is None or s.empty:
            continue

        if start:
            s = s.loc[pd.to_datetime(start):]
        if end:
            s = s.loc[:pd.to_datetime(end)]

        if len(s) < 2:
            continue

        ann_ret = (1 + s).prod() ** (freq / len(s)) - 1
        ann_vol = s.std(ddof=1) * np.sqrt(freq)
        sharpe = (ann_ret - rf) / ann_vol if ann_vol > 0 else np.nan

        cum = (1 + s).cumprod()
        dd = cum / cum.cummax() - 1
        maxdd = dd.min()

        rows.append({
            "factor": f,
            "ann_return": float(ann_ret),
            "ann_vol": float(ann_vol),
            "sharpe": None if not np.isfinite(sharpe) else float(sharpe),
            "maxdd": float(maxdd),
        })

    return {"rows": rows}
