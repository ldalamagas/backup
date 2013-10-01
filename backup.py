__author__ = 'ldalamagas'

import logging
import tarfile
import os
from datetime import datetime, timedelta
import ftplib
import re

# Backup configuration
backup_dirs = ["/home/ldalamagas/playground/backup"]
backup_prefix = "backup."
backup_suffix = ".tar.gz"
retention_period = None     # In days, None to deactivate the feature
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
        logging.info("transferring %s to %s/%s", tar_path, remote_host, remote_dir)
        ftp = ftplib.FTP(remote_host, remote_user, remote_password)
        ftp.cwd(remote_dir)
        f = open(tar_path, "rb")
        ftp.storbinary("".join(["STOR ", tar_file]), f)
    except ftplib.Error:
        logging.error("error while transferring %s archive to %s/%s", tar_path, remote_host, remote_dir)
        logging.warn("backup will now exit")
        ftp.close()
        exit(1)
    finally:
        f.close()

    # Delete old archives
    if retention_period is not None:
        try:
            nothing_deleted = True
            logging.info("deleting archives older than %i days", retention_period)
            file_listing = ftp.nlst()
            regex = re.compile(r"" + backup_prefix + "(\d{8})" + backup_suffix)

            for backup_file in file_listing:
                date_string = re.findall(regex, backup_file)
                backup_date = datetime.strptime(date_string[0], "%Y%m%d")
                if (start_time - backup_date) > timedelta(retention_period):
                    ftp.delete(backup_file)
                    nothing_deleted = False
                    logging.info("'%s' deleted", backup_file)

            if nothing_deleted:
                logging.info("nothing deleted")

        except ftplib.Error:
            logging.error("error while deleting old archives")
            logging.warn("backup will now exit")
            ftp.close()
            exit(1)
    else:
        logging.info("retention period is deactivated")
        ftp.close()

    duration = datetime.now() - start_time
    logging.info("backup ended in %s seconds", duration.total_seconds())
    exit(0)


if __name__ == "__main__":
   main()
