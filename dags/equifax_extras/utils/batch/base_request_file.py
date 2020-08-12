import os
from pathlib import Path


class BaseRequestFile:
    def __init__(self, path):
        self.path = path
        self.rows = []

        parent_dir = os.path.dirname(path)
        self.parent_dir = parent_dir
        self.filename = os.path.basename(path)
        self.qualified_path = os.path.join(self.parent_dir, self.filename)

        print(f"Creating file {self.qualified_path}")

        Path(parent_dir).mkdir(parents=True, exist_ok=True)
        file = open(self.path, "w+")
        file.close()

    def write(self, row):
        with open(self.path, "a+") as file:
            file.write(str(row))
