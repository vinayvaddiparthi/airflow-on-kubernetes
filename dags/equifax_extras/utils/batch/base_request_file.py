import os
from pathlib import Path


class BaseRequestFile:
    def __init__(self, path):
        self.path = path
        self.rows = []

        parent_dir = os.path.dirname(path)
        Path(parent_dir).mkdir(parents=True, exist_ok=True)
        file = open(self.path, "w+")
        file.close()

    def write(self, row):
        with open(self.path, "a+") as file:
            file.write(str(row))
