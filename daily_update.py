import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(r"C:\Users\admin\Desktop\factor-platform")
PYTHON_EXE = PROJECT_DIR / ".venv" / "Scripts" / "python.exe"

def run_cmd(cmd, cwd=PROJECT_DIR):
    print(f"\n>>> 執行: {' '.join(map(str, cmd))}")
    result = subprocess.run(cmd, cwd=cwd, check=True)
    return result

def main():
    # # 1. 更新 Excel / CMoney 資料
    # run_cmd([str(PYTHON_EXE), "xlsx_automation.py"])

    # # 2. 跑資料整理主流程
    # run_cmd([str(PYTHON_EXE), "run_all.py"])

    # 3. git add
    run_cmd(["git", "add", "."])

    # 4. git commit
    # 沒有變更時 commit 可能失敗，所以這裡可以容忍
    try:
        run_cmd(["git", "commit", "-m", "daily data update"])
    except subprocess.CalledProcessError:
        print(">>> 沒有可提交的變更，略過 commit")

    # 5. git push
    run_cmd(["git", "push"])

if __name__ == "__main__":
    try:
        main()
        print("\n✅ 全部流程完成")
    except Exception as e:
        print(f"\n❌ 流程失敗: {e}")
        sys.exit(1)