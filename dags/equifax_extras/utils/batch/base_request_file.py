from datetime import datetime
from pathlib import Path
import os

from .base_row import BaseRow


class BaseRequestFile:
    def __init__(self, path: str):
        self.path = path
        self.rows = list()

        parent_dir = os.path.dirname(path)
        self.parent_dir = parent_dir
        self.filename = os.path.basename(path)
        self.qualified_path = os.path.join(self.parent_dir, self.filename)

        self.timestamp = datetime.now()
        self.encoding = "cp1252"

        print(f"Creating file {self.qualified_path}")

        Path(parent_dir).mkdir(parents=True, exist_ok=True)
        file = open(self.path, "w+", encoding=self.encoding)
        file.close()

    def write_header(self) -> None:
        time_tag = self.timestamp.strftime("%Y%m%d%H%M%S")
        header = f"BHDR-EQUIFAX{time_tag}ADVITFINSCOREDA2\n"
        with open(self.path, "a+", encoding=self.encoding) as file:
            file.write(header)

    def write_footer(self) -> None:
        time_tag = self.timestamp.strftime("%Y%m%d%H%M%S")
        trailer = f"BTRL-EQUIFAX{time_tag}ADVITFINSCOREDA2\n"
        with open(self.path, "a+", encoding=self.encoding) as file:
            file.write(trailer)

    def write(self, row: BaseRow) -> None:
        with open(self.path, "a+", encoding=self.encoding) as file:
            file.write(str(row))
