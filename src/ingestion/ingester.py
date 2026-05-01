from pathlib import Path
import pandas as pd

class CSVIngester:
    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)

    def load_csv(self, relative_path: str, **kwargs) -> pd.DataFrame:
        path = self.base_dir / relative_path
        if not path.exists():
            raise FileNotFoundError(f"Missing file: {path}")
        return pd.read_csv(path, **kwargs)
