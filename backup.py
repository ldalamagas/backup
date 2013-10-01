__author__ = 'ldalamagas'

import logging
import tarfile
import os
from datetime import datetime
import ftplib
import re

# Logging Facility
logging_facility = "python"     # Can be python or syslog

# What to backup
backup_dirs = ["/home/ldalamagas/playground/backup"]
backup_prefix = "backup."
backup_suffix = ".tar.gz"
tmp_dir = "/tmp"

# Remote Storage
remote_host = "ftp.imc.com.gr"
remote_dir = "python_backup"
remote_user = "developer"
remote_password = "developer"


def get_tar_name():
    return ''.join([backup_prefix, datetime.now().date().strftime("%Y%m%d"), backup_suffix])


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    start_time = datetime.now()
    logging.info("starting backup %s", start_time.time().strftime("%H:%M:%S"))
    tar_file = get_tar_name()
    tar_path = os.path.join(tmp_dir, tar_file)

    try:
        logging.info("archiving files")
        tar = tarfile.open(tar_path, "w:gz")
        for dir in backup_dirs:
            tar.add(dir)
        tar.close()
    except IOError:
        logging.error("error while creating tar archive")
        logging.warn("backup will now exit")
        exit(1)

    try:
        logging.info("transferring %s", tar_path)
        ftp = ftplib.FTP(remote_host, remote_user, remote_password)
        ftp.cwd(remote_dir)
        # file = open(tar_path, "rb")
        # ftp.storbinary("".join(["STOR ", tar_file]), file)
        files = ftp.nlst()
        print files
    except ftplib.Error:
        logging.error("Error while transferring %s archive to %s", tar_path, remote_host)
        logging.warn("backup will now exit")
        exit(1)
    finally:
        # file.close()
        ftp.close()

    duration = datetime.now() - start_time
    logging.info("backup ended in %s seconds", duration.total_seconds())
    exit(0)


if __name__ == "__main__":
   main()
