from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from fs.sshfs import SSHFS
from fs.tools import copy_file_data
from fs_s3fs import S3FS


def sync_s3fs_to_sshfs(aws_conn: str, sshfs_conn: str):
    aws_conn = AwsHook.get_connection(aws_conn)
    ssh_conn = SSHHook.get_connection(sshfs_conn)

    sshfs = SSHFS(host=ssh_conn.host, user=ssh_conn.login, passwd=ssh_conn.password)

    s3fs = S3FS(
        bucket_name=aws_conn.extra_dejson["bucket"],
        region=aws_conn.extra_dejson["region"],
        dir_path=aws_conn.extra_dejson["dir_path"],
    )

    local_files = s3fs.listdir("inbox")

    for file in local_files:
        with s3fs.open(f"outbox/{file}", "rb") as origin_file, sshfs.open(
            f"inbox/{file}", "wb"
        ) as remote_file:
            copy_file_data(origin_file, remote_file)
            s3fs.remove(f"outbox/{file}")


def sync_sshfs_to_s3fs(aws_conn: str, sshfs_conn: str):
    aws_conn = AwsHook.get_connection(aws_conn)
    ssh_conn = SSHHook.get_connection(sshfs_conn)

    sshfs = SSHFS(host=ssh_conn.host, user=ssh_conn.login, passwd=ssh_conn.password)

    s3fs = S3FS(
        bucket_name=aws_conn.extra_dejson["bucket"],
        region=aws_conn.extra_dejson["region"],
        dir_path=aws_conn.extra_dejson["dir_path"],
    )

    remote_files = sshfs.listdir("outbox")
    local_files = set(s3fs.listdir("inbox"))

    for file in (file for file in remote_files if file not in local_files):
        with sshfs.open(f"outbox/{file}", "rb") as origin_file, s3fs.open(
            f"inbox/{file}", "wb"
        ) as dest_file:
            copy_file_data(origin_file, dest_file)


if __name__ == "__main__":
    passwd = "d@t@!!1234"
