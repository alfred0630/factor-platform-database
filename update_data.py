import pandas as pd
import numpy as np
import re
import os
import clean_data  # ✅ 直接匯入你的 clean_data.py

# ==========================================
# 1. 定義「更新因子.xlsx」專用的客製化函式
# (這些是你原本寫在 Notebook cell 裡的，clean_data 裡可能沒有)
# ==========================================

def clean_price_local(df):
    """ 本地版 clean_price (針對更新檔的切片格式) """
    df = df.copy()
    first_col = df.columns[0]
    df["date"] = df[first_col].astype(str).str.extract(r"(\d{8})", expand=False)
    df = df.dropna(subset=["date"])
    df = df.set_index("date")
    df.index.name = "date"
    code_cols = [c for c in df.columns if re.fullmatch(r"\d{4,6}", str(c))]
    df = df[code_cols].apply(pd.to_numeric, errors="coerce")
    return df

def clean_eps_local(df):
    """ 本地版 clean_eps """
    df = df.copy()
    first_col = df.columns[0]
    df["period"] = df[first_col].astype(str).str.extract(r"(\d{6})", expand=False)
    df = df.dropna(subset=["period"])
    df = df.set_index("period")
    df.index.name = "period"
    code_cols = [c for c in df.columns if re.fullmatch(r"\d{4,6}", str(c))]
    df = df[code_cols].apply(pd.to_numeric, errors="coerce")
    return df

def clean_code_table_ready_local(df):
    """ 本地版表格清洗 """
    df = df.copy()
    code_cols = [c for c in df.columns if re.fullmatch(r"\d{4,6}", str(c))]
    if not code_cols: return pd.DataFrame()
    
    first_col = df.columns[0]
    def extract_period(s):
        s = str(s)
        m = re.search(r"(\d{4})Q([1-4])", s)
        if m: return f"{m.group(1)}0{m.group(2)}"
        m = re.search(r"(\d{6})", s)
        if m: return m.group(1)
        return None
    
    df["period"] = df[first_col].map(extract_period)
    df = df.dropna(subset=["period"])
    df[code_cols] = df[code_cols].apply(pd.to_numeric, errors="coerce")
    out = df.groupby("period")[code_cols].mean().sort_index()
    out.index.name = "period"
    out.columns = [str(c) for c in out.columns]
    return out

def to_ym_by_code(df):
    """ 針對 '更新因子' 的特殊格式清洗 """
    df = df.copy()
    first_col = df.columns[0]
    ym = df[first_col].astype(str).str.extract(r'(\d{6})', expand=False)
    mask = ym.notna()
    ym = ym[mask].astype(int)
    code_cols = [c for c in df.columns if re.fullmatch(r'\d{4,6}', str(c))]
    if not code_cols: return pd.DataFrame()
    
    values = df.loc[mask.index[mask], code_cols].apply(pd.to_numeric, errors='coerce')
    values.index = ym.values
    out = values.groupby(values.index).mean().sort_index()
    out.index.name = "period"
    out.columns = [str(c) for c in out.columns]
    return out


# ==========================================
# 2. 主程式執行
# ==========================================

# 建立字典收集所有結果
output_dict = {}

# --- 處理 1: 更新因子.xlsx (使用上面的本地函式) ---
if os.path.exists("更新因子.xlsx"):
    print("📂 正在處理：更新因子.xlsx ...")
    try:
        # Helper to read and slice
        def load_slice(sheet):
            return pd.read_excel("更新因子.xlsx", sheet_name=sheet).iloc[:, 4:].drop(index=0, axis=0)

        output_dict["pe_new"]     = to_ym_by_code(load_slice("本益比"))
        output_dict["pb_new"]     = to_ym_by_code(load_slice("pb"))
        output_dict["beta_new"]   = to_ym_by_code(load_slice("Beta"))
        output_dict["mv_new"]     = to_ym_by_code(load_slice("市值_"))
        output_dict["yields_new"] = to_ym_by_code(load_slice("殖利率"))
        
        output_dict["gross_new"]  = clean_code_table_ready_local(load_slice("毛利率"))
        output_dict["rev_new"]    = clean_code_table_ready_local(load_slice("營業利益率"))
        output_dict["rev_month_new"] = clean_eps_local(load_slice("月營收"))
        output_dict["cleaned_eps_new"] = clean_eps_local(load_slice("預估eps"))
        
        price_raw = pd.read_excel("更新因子.xlsx", sheet_name="收盤價")
        output_dict["cleaned_price_new"] = clean_price_local(price_raw.iloc[:, 4:].drop(index=0, axis=0))
        
        print("✔ 更新因子部分完成")
    except Exception as e:
        print(f"❌ 更新因子部分失敗: {e}")


# --- 處理 2: 因子資料全.xlsx (使用 clean_data.py) ---
if os.path.exists("因子資料全.xlsx"):
    print("\n📂 正在處理：因子資料全.xlsx ...")
    try:
        # ✅ 這裡使用你 clean_data.py 裡面的函式
        output_dict["price"]    = clean_data.clean_price(pd.read_excel("因子資料全.xlsx", sheet_name="收盤價"))
        output_dict["mktcap"]   = clean_data.clean_mktcap(pd.read_excel("因子資料全.xlsx", sheet_name="市值"))
        output_dict["pe_ratio"] = clean_data.clean_mktcap(pd.read_excel("因子資料全.xlsx", sheet_name="低本益比"))
        output_dict["pb_ratio"] = clean_data.clean_mktcap(pd.read_excel("因子資料全.xlsx", sheet_name="低PB"))
        output_dict["yd"]       = clean_data.clean_mktcap(pd.read_excel("因子資料全.xlsx", sheet_name="殖利率"))
        output_dict["beta"]     = clean_data.clean_mktcap(pd.read_excel("因子資料全.xlsx", sheet_name="Beta"))
        output_dict["earn_yoy"] = clean_data.clean_mktcap(pd.read_excel("因子資料全.xlsx", sheet_name="月營收"))
        output_dict["gross"]    = clean_data.clean_mktcap(pd.read_excel("因子資料全.xlsx", sheet_name="毛利率"))
        output_dict["rev"]      = clean_data.clean_mktcap(pd.read_excel("因子資料全.xlsx", sheet_name="營利率"))
        output_dict["eps"]      = clean_data.clean_mktcap(pd.read_excel("因子資料全.xlsx", sheet_name="月預估EPS"))

        if "price" in output_dict and not output_dict["price"].empty:
            output_dict["returns"] = output_dict["price"].pct_change()

        print("✔ 因子資料全部分完成")
    except Exception as e:
        print(f"❌ 因子資料全部分失敗: {e}")
        print("💡 提示：請檢查 clean_data.py 中的 clean_mktcap 是否能處理該檔案的格式")


# ==========================================
# 3. 輸出結果
# ==========================================
output_folder = "merged_csvs"
os.makedirs(output_folder, exist_ok=True)
print(f"\n--- 開始存檔至 {output_folder} ---")

for name, df in output_dict.items():
    if not isinstance(df, pd.DataFrame) or df.empty:
        continue
        
    df_to_save = df.copy()
    
    # Index 格式化 (Datetime -> String)
    if isinstance(df_to_save.index, pd.DatetimeIndex):
        fmt = "%Y-%m-%d" if "price" in name else "%Y-%m"
        df_to_save.index = df_to_save.index.strftime(fmt)
    else:
        # 若 Index 為數字字串 (如 202501)，且不是日資料
        if "price" not in name and "returns" not in name:
            idx_str = df_to_save.index.astype(str).str.strip()
            # 簡單防呆：如果是 6 碼數字就切分
            try:
                if len(idx_str) > 0 and idx_str[0].isdigit() and len(idx_str[0]) == 6:
                    df_to_save.index = idx_str.str[:4] + "-" + idx_str.str[4:6]
            except:
                pass

    path = os.path.join(output_folder, f"{name}.csv")
    df_to_save.to_csv(path, encoding="utf-8-sig")
    print(f"✔ {name}.csv")

print("\n✅ 全部執行完畢")