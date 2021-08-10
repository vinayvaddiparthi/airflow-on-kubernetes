from airflow.models import Variable

from pathlib import Path
import gnupg
from typing import List


def _init_gnupg() -> gnupg.GPG:
    path_ = Path("~/.gnupg")
    path_.mkdir(parents=True, exist_ok=True)
    gpg = gnupg.GPG(gnupghome=path_)

    keys: List[str] = Variable.get("equifax_pgp_keys", deserialize_json=True)
    for key in keys:
        gpg.import_keys(key)

    return gpg
