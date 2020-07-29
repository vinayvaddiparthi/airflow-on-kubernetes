from typing import Any

import pysftp
import gnupg
from airflow.hooks.base_hook import BaseHook


def encrypt(filename: str) -> Any:
    equifax_hook = BaseHook.get_connection("equifax_pgp_pub")
    recipient = (equifax_hook.host,)
    pgp_public_key = equifax_hook.password
    passphrase = equifax_hook.extra_dejson.get("passphrase")

    gpg = gnupg.GPG(gnupghome=".")
    # clear keys in cwd
    for key in gpg.list_keys(True).fingerprints:
        gpg.delete_keys(key, True, passphrase=passphrase)
    for key in gpg.list_keys().fingerprints:
        gpg.delete_keys(key)

    import_result = gpg.import_keys(pgp_public_key)
    print(f"PGP key import status: {import_result.ok}")

    with open(filename, "rb") as f:
        status = gpg.encrypt_file(
            f,
            symmetric=True,
            recipients=[recipient],
            always_trust=True,
            passphrase=passphrase,
            output=f"{filename}.pgp",
        )
        print("ok: ", status.ok)
        print("status: ", status.status)
        print("stderr: ", status.stderr)
        if status.ok:
            return f"{filename}.pgp"
        else:
            return None


def decrypt_file(filename: str) -> Any:
    output = filename.replace(".pgp", "")
    equifax_hook = BaseHook.get_connection("equifax_pgp_secret")
    pgp_private_key = equifax_hook.password
    passphrase = equifax_hook.extra_dejson.get("passphrase")

    gpg = gnupg.GPG(gnupghome=".")
    # clear keys in cwd
    for key in gpg.list_keys(True).fingerprints:
        gpg.delete_keys(key, True, passphrase=passphrase)
    for key in gpg.list_keys().fingerprints:
        gpg.delete_keys(key)

    import_result = gpg.import_keys(pgp_private_key)
    print(f"PGP key import status: {import_result.ok}")

    with open(filename, "rb") as f:
        status = gpg.decrypt_file(f, passphrase=passphrase, output=output)
        print("ok: ", status.ok)
        print("status: ", status.status)
        print("stderr: ", status.stderr)

    with open(output, "rb") as file:
        if file:
            print("ok")
            return output
        else:
            print("not exist")
            return None


def download(host: str, username: str, password: str) -> list:
    """
    Go onto SFTP endpoint and fetch what's in outbox folder
    :param host: sftp host, save in airflow connection: equifax_sftp
    :param username: sftp username, save in airflow connection: equifax_sftp
    :param password: sftp password, save in airflow connection: equifax_sftp
    :return: files downloaded
    """
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    try:
        with pysftp.Connection(
            host, username=username, password=password, cnopts=cnopts
        ) as sftp, sftp.cd("/outbox"):
            files = sftp.listdir()
            print("File available in outbox:")
            for file in files:
                print(f"[{file}]")

            sftp.get_d(".", ".", preserve_mtime=True)
            return files
    except Exception as e:
        print(e)
        raise e


def upload(host: str, username: str, password: str, filename: str) -> list:
    """
    Upload file into SFTP's inbox folder
    :param filename: the filename of encrypted request file
    :param host: sftp host, save in airflow connection: equifax_sftp
    :param username: sftp username, save in airflow connection: equifax_sftp
    :param password: sftp password, save in airflow connection: equifax_sftp
    :return: files in inbox
    """
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    try:
        with pysftp.Connection(
            host, username=username, password=password, cnopts=cnopts
        ) as sftp, sftp.cd("/inbox"):
            sftp.put(filename, preserve_mtime=True)
            with sftp.cd("/inbox"):
                files = sftp.listdir()
            return files
    except Exception as e:
        print(e)
        raise e
