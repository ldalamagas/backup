import subprocess

__author__ = 'ldalamagas'

import logging
import tarfile
import os
from datetime import datetime, timedelta
import ftplib
import re

# Backup configuration
backup_items = ["/home/ldalamagas/playground/backup"]
backup_prefix = "backup."
backup_suffix = ".tar.gz"
retention_period = 0     # In days, None to disable the feature
tmp_dir = "/tmp"

# MySQL BackUp
db_names = ["opinions"]        # None to disable the feature
db_host = "localhost"
db_username = "opinions"
db_password = "opinions"

# Remote Storage
remote_host = "ftp.imc.com.gr"
remote_dir = "python_backup"
remote_user = "developer"
remote_password = "developer"


def get_tar_name():
    return ''.join([backup_prefix, datetime.now().date().strftime("%Y%m%d"), backup_suffix])


def on_error(start_time):
    duration = datetime.now() - start_time
    logging.warn("backup will now exit")
    logging.info("backup ended in %s seconds", duration.total_seconds())
    exit(1)


def main():
    cleanup = []
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    start_time = datetime.now()
    logging.info("starting backup %s", start_time.time().strftime("%H:%M:%S"))
    tar_file = get_tar_name()
    tar_path = os.path.join(tmp_dir, tar_file)

    # Dump MySQL databases
    if db_names is not None:
        try:
            logging.info("dumping databases")
            for db_name in db_names:
                logging.info("dumping database %s", db_name)
                cmd = "mysqldump --single-transaction --host %s -u %s -p%s %s" \
                      % (db_host, db_username, db_password, db_name)
                dumpfile_name = os.path.join(tmp_dir, ".".join([db_name, "sql"]))
                backup_items.append(dumpfile_name)      # Include it to the backup archive
                cleanup.append(dumpfile_name)           # We are gonna need this to cleanup later
                dumpfile = open(dumpfile_name, "w")     # Sample: /tmp/database.sql
                subprocess.check_call(cmd, stdout=dumpfile, shell=True)
                dumpfile.close()

        except subprocess.CalledProcessError:
            logging.error("error while dumping mysql databases, the error occurred while calling the mysqldump process")
            on_error(start_time)
        except OSError:
            logging.error("error while dumping mysql databases, does the output file/directory exist?")
            on_error(start_time)
    else:
        logging.info("mysql backup is disabled")

    # Archive the directories
    try:
        logging.info("archiving files %s", backup_items)
        tar = tarfile.open(tar_path, "w:gz")
        for item in backup_items:
            tar.add(item)
        tar.close()
        cleanup.append(tar_path)
    except IOError:
        logging.error("error while creating tar archive")
        on_error(start_time)

    # Transfer archive to remote destination
    try:
        logging.info("transferring %s to %s/%s", tar_path, remote_host, remote_dir)
        ftp = ftplib.FTP(remote_host, remote_user, remote_password)
        ftp.cwd(remote_dir)
        f = open(tar_path, "rb")
        ftp.storbinary("".join(["STOR ", tar_file]), f)
    except ftplib.Error:
        logging.error("error while transferring %s archive to %s/%s", tar_path, remote_host, remote_dir)
        ftp.close()
        on_error(start_time)
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
            ftp.close()
            on_error(start_time)
    else:
        logging.info("retention period is disabled")
        ftp.close()

    # Clean up!
    try:
        logging.info("cleaning up, deleting %s", cleanup)
        for item in cleanup:
            os.remove(item)
    except OSError:
        logging.error("error while performing cleanup, %s is a directory")
    except:
        logging.error("error while performing cleanup")

    duration = datetime.now() - start_time
    logging.info("backup ended in %s seconds", duration.total_seconds())
    exit(0)


if __name__ == "__main__":
    main()
