#!/usr/bin/python
import ConfigParser
from ConfigParser import NoOptionError
import email
from smtplib import SMTPAuthenticationError
import socket
import subprocess
import logging
import logging.handlers
import tarfile
import os
from datetime import datetime, timedelta
import ftplib
import re
import smtplib
from email.mime.text import MIMEText
import argparse
import platform

__author__ = 'ldalamagas'

config = {}
os_platform = platform.system()

logger = logging.getLogger("backup")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("BACKUP[%(process)d] %(levelname)s: %(message)s")
syslog_handler = None       # To avoid the used before assignment warning
if os_platform == "Linux":
    syslog_handler = logging.handlers.SysLogHandler(address="/dev/log")
elif os_platform == "Windows":
    syslog_handler = logging.handlers.NTEventLogHandler("Hoarder")
stream_handler = logging.StreamHandler()
syslog_handler.setFormatter(fmt=formatter)
stream_handler.setFormatter(fmt=formatter)
logger.addHandler(syslog_handler)
logger.addHandler(stream_handler)

start_time = datetime.now()


def read_config(configuration_file, config):
    logger.info("running backup with [%s] configuration", configuration_file)
    cp = ConfigParser.ConfigParser()
    try:
        cp.readfp(open(configuration_file))

        # General backup configuration
        config["backup_items"] = (cp.get("backup", "items")).split(",")
        config["backup_prefix"] = cp.get("backup", "prefix")
        config["backup_suffix"] = cp.get("backup", "suffix")
        config["retention_enabled"] = cp.getboolean("backup", "retention_enabled")
        config["retention_period"] = cp.getint("backup", "retention")
        config["tmp_dir"] = cp.get("backup", "temp_storage")

        # MySQL
        config["db_enabled"] = cp.getboolean("mysql", "enabled")
        config["db_names"] = (cp.get("mysql", "names")).split(",")
        config["db_host"] = cp.get("mysql", "host")
        config["db_user"] = cp.get("mysql", "user")
        config["db_password"] = cp.get("mysql", "password")

        # Remote Storage
        config["ftp_host"] = cp.get("ftp", "host")
        config["ftp_dir"] = cp.get("ftp", "dir")
        config["ftp_user"] = cp.get("ftp", "user")
        config["ftp_password"] = cp.get("ftp", "password")
        config["ftp_passive"] = cp.get("ftp", "passive")

        # Mail Notifications
        config["smtp_enabled"] = cp.getboolean("smtp", "enabled")
        config["smtp_server"] = cp.get("smtp", "server")
        config["smtp_from_address"] = cp.get("smtp", "from")
        config["smtp_to_address"] = cp.get("smtp", "to")
        config["smtp_user"] = cp.get("smtp", "user")
        config["smtp_password"] = cp.get("smtp", "password")

    except IOError as error:
        on_error(error, "error while reading configuration, does the file exist?")
    except NoOptionError as error:
        message = "option [%s] does not exist in section [%s], " \
                  "please review your configuration file" % (error.option, error.section)
        on_error(error, message)


def send_mail(message):
    smtp = None
    try:
        msg = MIMEText(message)
        msg['Subject'] = 'Error while backing up [%s]' % socket.gethostname()
        msg['From'] = config["smtp_from_address"]
        msg['Reply-To'] = config["smtp_from_address"]
        msg['To'] = config["smtp_to_address"]

        smtp = smtplib.SMTP(config["smtp_server"])
        smtp.starttls()
        smtp.login(config["smtp_user"], config["smtp_password"])
        smtp.sendmail(config["smtp_from_address"], config["smtp_to_address"], msg.as_string())
    except email.errors.MessageError:
        logger.error("Error trying to notify recipients")
    except SMTPAuthenticationError:
        logger.error("Error trying to notify recipients, please check your smtp credentials")
    finally:
        smtp.quit()


def on_error(error, message, cleanup=None):
    duration = datetime.now() - start_time

    if error and hasattr(error, "output"):
        message = "".join([message, ". The error was: ", error.output.rstrip("\n")])
        logger.error(message)
    elif error and hasattr(error, "strerror"):
        message = "".join([message, ". The error was: ", error.strerror.rstrip("\n")])
        logger.error(message)
    elif error and hasattr(error, "message"):
        message = "".join([message, ". The error was: ", error.message.rstrip("\n")])
        logger.error(message)
    else:
        logger.error(message)

    if "smtp_enabled" is config.keys() and config["smtp_enabled"]:
        send_mail(message)

    if cleanup is not None:
        perform_cleanup(cleanup)

    logger.warning("backup ended with errors in %s seconds", duration.total_seconds())
    exit(1)


def perform_cleanup(items):
    # Clean up!
    if len(items) > 0:
        logger.info("cleaning up, deleting %s", items)
        for item in items:
            try:
                os.remove(item)
            except OSError as error:
                message = "error while performing cleanup"
                on_error(error, message)
    else:
        logger.info("nothing to clean up")


def backup():
    cleanup = []
    parser = argparse.ArgumentParser(description="Hoarder Back Up")
    parser.add_argument('-c', '--config', default="backup.cfg", help="Path to the configuration file")
    arguments = parser.parse_args()

    read_config(arguments.config, config)

    logger.info("starting backup %s", start_time.time().strftime("%H:%M:%S"))
    tar_file = ''.join([config["backup_prefix"], datetime.now().date().strftime("%Y%m%d"), config["backup_suffix"]])
    tar_path = os.path.join(config["tmp_dir"], tar_file)

    # Dump MySQL databases
    if config["db_enabled"]:
        try:
            logger.info("dumping databases")
            for db_name in config["db_names"]:
                logger.info("dumping database %s", db_name)
                dumpfile_name = os.path.join(config["tmp_dir"], ".".join([db_name, "sql"]))
                cmd = "mysqldump --single-transaction --host %s -u %s -p%s %s > %s" \
                      % (config["db_host"], config["db_user"], config["db_password"], db_name, dumpfile_name)
                config["backup_items"].append(dumpfile_name)    # Include it to the backup archive
                cleanup.append(dumpfile_name)                   # We are gonna need this to cleanup later
                subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as error:
            on_error(error, "error while dumping mysql databases, "
                            "the error occurred while calling the mysqldump process", cleanup)
        except OSError as error:
            on_error(error, "error while dumping mysql databases, does the output file/directory exist?", cleanup)
    else:
        logger.info("mysql backup is disabled")

    # Archive the directories
    try:
        logger.info("archiving %s", config["backup_items"])
        tar = tarfile.open(tar_path, "w:gz")
        for item in config["backup_items"]:
            tar.add(item)
        tar.close()
        cleanup.append(tar_path)
    except (IOError, OSError) as error:
        on_error(error, "error while creating tar archive", cleanup)

    # Transfer archive to remote destination
    ftp = ftplib.FTP()

    ftp.set_pasv(config["ftp_passive"])     # Transfer fails on some servers while passive enabled

    f = None
    try:
        logger.info("transferring %s to %s/%s", tar_path, config["ftp_host"], config["ftp_dir"])
        ftp.connect(config["ftp_host"])
        ftp.login(config["ftp_user"], config["ftp_password"])
        ftp.cwd(config["ftp_dir"])
        f = open(tar_path, "rb")
        ftp.storbinary("".join(["STOR ", tar_file]), f)
    except ftplib.Error as error:
        ftp.close()
        message = "error while transferring %s archive to %s/%s" % (tar_path, config["ftp_host"], config["ftp_dir"])
        on_error(error, message, cleanup)
    finally:
        if f is not None:
            f.close()

    # Delete old archives
    if config["retention_enabled"]:
        try:
            nothing_deleted = True
            logger.info("deleting archives older than %i days", config["retention_period"])
            file_listing = ftp.nlst()
            regex = re.compile(r"" + config["backup_prefix"] + "(\d{8})" + config["backup_suffix"])

            for backup_file in file_listing:
                date_string = re.findall(regex, backup_file)
                backup_date = datetime.strptime(date_string[0], "%Y%m%d")
                if (start_time - backup_date) > timedelta(config["retention_period"]):
                    ftp.delete(backup_file)
                    nothing_deleted = False
                    logger.info("'%s' deleted", backup_file)

            if nothing_deleted:
                logger.info("nothing deleted")
        except ftplib.Error as error:
            ftp.close()
            on_error(error, "error while deleting old archives", cleanup)
    else:
        logger.info("retention period is disabled")
        ftp.close()

    perform_cleanup(cleanup)

    duration = datetime.now() - start_time
    logger.info("backup ended in %s seconds", duration.total_seconds())
    exit(0)


if __name__ == "__main__":
    backup()
