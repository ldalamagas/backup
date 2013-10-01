#!/usr/bin/python
import ConfigParser
import subprocess
import logging
import tarfile
import os
from datetime import datetime, timedelta
import ftplib
import re

__author__ = 'ldalamagas'

config = {}


def read_config(config):

    cp = ConfigParser.ConfigParser()
    cp.readfp(open("backup.cfg"))

    # General backup configuration
    config["backup_items"] = (cp.get("general", "backup_items")).split(",")
    config["backup_prefix"] = cp.get("general", "backup_prefix")
    config["backup_suffix"] = cp.get("general", "backup_suffix")
    config["retention_period"] = cp.getint("general", "retention_period")
    config["tmp_dir"] = cp.get("general", "tmp_dir")

    # MySQL
    config["db_names"] = (cp.get("general", "db_names")).split(",")
    config["db_host"] = cp.get("general", "db_host")
    config["db_user"] = cp.get("general", "db_user")
    config["db_password"] = cp.get("general", "db_password")

    # Remote Storage
    config["ftp_host"] = cp.get("general", "ftp_host")
    config["ftp_dir"] = cp.get("general", "ftp_dir")
    config["ftp_user"] = cp.get("general", "ftp_user")
    config["ftp_password"] = cp.get("general", "ftp_password")


def get_tar_name():
    return ''.join([config["backup_prefix"], datetime.now().date().strftime("%Y%m%d"), config["backup_suffix"]])


def on_error(start_time):
    duration = datetime.now() - start_time
    logging.warn("backup will now exit")
    logging.info("backup ended in %s seconds", duration.total_seconds())
    exit(1)


def main():
    read_config(config)
    cleanup = []
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    start_time = datetime.now()
    logging.info("starting backup %s", start_time.time().strftime("%H:%M:%S"))
    tar_file = get_tar_name()
    tar_path = os.path.join(config["tmp_dir"], tar_file)

    # Dump MySQL databases
    if config["db_names"] is not "-1":
        try:
            logging.info("dumping databases")
            for db_name in config["db_names"]:
                logging.info("dumping database %s", db_name)
                cmd = "mysqldump --single-transaction --host %s -u %s -p%s %s" \
                      % (config["db_host"], config["db_user"], config["db_password"], db_name)
                dumpfile_name = os.path.join(config["tmp_dir"], ".".join([db_name, "sql"]))
                config["backup_items"].append(dumpfile_name)      # Include it to the backup archive
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
        logging.info("archiving %s", config["backup_items"])
        tar = tarfile.open(tar_path, "w:gz")
        for item in config["backup_items"]:
            tar.add(item)
        tar.close()
        cleanup.append(tar_path)
    except IOError:
        logging.error("error while creating tar archive")
        on_error(start_time)

    # Transfer archive to remote destination
    ftp = ftplib.FTP()
    f = None
    try:
        logging.info("transferring %s to %s/%s", tar_path, config["ftp_host"], config["ftp_dir"])
        ftp.connect(config["ftp_host"])
        ftp.login(config["ftp_user"], config["ftp_password"])
        ftp.cwd(config["ftp_dir"])
        f = open(tar_path, "rb")
        ftp.storbinary("".join(["STOR ", tar_file]), f)
    except ftplib.Error:
        logging.error("error while transferring %s archive to %s/%s", tar_path, config["ftp_host"], config["ftp_dir"])
        ftp.close()
        on_error(start_time)
    finally:
        if f is not None:
            f.close()

    # Delete old archives
    if config["retention_period"] is not -1:
        try:
            nothing_deleted = True
            logging.info("deleting archives older than %i days", config["retention_period"])
            file_listing = ftp.nlst()
            regex = re.compile(r"" + config["backup_prefix"] + "(\d{8})" + config["backup_suffix"])

            for backup_file in file_listing:
                date_string = re.findall(regex, backup_file)
                backup_date = datetime.strptime(date_string[0], "%Y%m%d")
                if (start_time - backup_date) > timedelta(config["retention_period"]):
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
    logging.info("cleaning up, deleting %s", cleanup)
    for item in cleanup:
        try:
            os.remove(item)
        except OSError:
            logging.error("error while performing cleanup, %s is a directory", item)
            on_error(start_time)

    duration = datetime.now() - start_time
    logging.info("backup ended in %s seconds", duration.total_seconds())
    exit(0)


if __name__ == "__main__":
    main()
