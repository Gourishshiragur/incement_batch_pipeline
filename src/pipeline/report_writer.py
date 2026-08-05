import json
from pathlib import Path
from datetime import datetime


def write_report(report_name: str, data: dict):

    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)

    data["generated_at"] = datetime.now().isoformat()

    report_file = reports_dir / report_name

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    return report_file
