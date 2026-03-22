"""
Module 3 v1.2.0: Record dbt logic version for MLflow registry.

Requirement: dbt_logic_version = git rev-parse HEAD for the dbt models folder.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


def get_dbt_logic_version(repo_root: Path) -> str:
    models_dir = repo_root / "dbt" / "models"
    return (
        subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(models_dir))
        .decode()
        .strip()
    )


if __name__ == "__main__":
    print(get_dbt_logic_version(Path(__file__).resolve().parents[1]))

