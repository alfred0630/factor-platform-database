import subprocess
import sys

scripts = [
    "update_data.py",
    "scripts/export_returns.py",
    "scripts/export_heatmap.py",
    "scripts/export_global_wave.py",
]

for script in scripts:
    print(f"\n=== Running {script} ===")
    
    result = subprocess.run([sys.executable, script])
    
    if result.returncode != 0:
        print(f"Error running {script}")
        break

print("\nAll scripts finished.")