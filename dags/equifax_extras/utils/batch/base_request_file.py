from datetime import datetime
from pathlib import Path
import logging


class BaseRequestFile:
    def __init__(self, path: Path):
        self.path = path
        self.timestamp = datetime.now()
        self.encoding = "utf-8"
        self.rows = 0

        logging.info(f"Creating file {self.path}")
        with open(self.path, "w+", encoding=self.encoding):
            pass

    def write_header(self) -> None:
        pass

    def write_footer(self) -> None:
        pass

    def write(self, row: str) -> None:
        with open(self.path, "a+", encoding=self.encoding) as file:
            file.write(row)

        self.rows = self.rows + 1
