import os
import shutil
import subprocess
import sys
from pathlib import Path
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def run_step(label, command):
    print(f"[START] {label}")
    try:
        subprocess.run(command, check=True)
        print(f"[OK] {label}")
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] {label} failed with exit code {exc.returncode}")
        sys.exit(exc.returncode)
    except FileNotFoundError:
        print(f"[ERROR] Command not found: {command[0]}")
        sys.exit(1)


def _find_spark_submit():
    env_path = os.getenv("SPARK_SUBMIT")
    if env_path and Path(env_path).exists():
        return env_path

    which_path = shutil.which("spark-submit") or shutil.which("spark-submit.cmd")
    if which_path:
        return which_path

    try:
        import pyspark
        pyspark_root = Path(pyspark.__file__).resolve().parent
        candidate = pyspark_root / "bin" / "spark-submit.cmd"
        if candidate.exists():
            return str(candidate)
    except Exception:
        pass

    return None


def main():
    create_tables = [PYTHON, str(ROOT / "src" / "create_tables.py")]

    aggregate_job = str(ROOT / "spark" / "aggregate_hourly.py")
    spark_submit = _find_spark_submit()
    if spark_submit:
        spark_cmd = [spark_submit, aggregate_job]
    else:
        print("[WARN] spark-submit not found, running aggregation with python")
        spark_cmd = [PYTHON, aggregate_job]

    analytics = [PYTHON, str(ROOT / "src" / "analytics.py")]

    steps = [
        ("Create tables", create_tables),
        ("Spark aggregation", spark_cmd),
        ("Run analytics", analytics),
    ]

    for label, command in steps:
        run_step(label, command)

    print("[DONE] Pipeline completed successfully")


if __name__ == "__main__":
    main()
