# -*- coding: utf-8 -*-
#
# Univention Management Console
#  logging module for UMC
#
# Like what you see? Join us!
# https://www.univention.com/about-us/careers/vacancies/
#
# Copyright 2011-2023 Univention GmbH
#
# https://www.univention.de/
#
# All rights reserved.
#
# The source code of this program is made available
# under the terms of the GNU Affero General Public License version 3
# (GNU AGPL V3) as published by the Free Software Foundation.
#
# Binary versions of this program provided by Univention to you as
# well as other copyrighted, protected or trademarked materials like
# Logos, graphics, fonts, specific documentations and configurations,
# cryptographic keys etc. are subject to a license agreement between
# you and Univention and not subject to the GNU AGPL V3.
#
# In the case you use this program under the terms of the GNU AGPL V3,
# the program is provided in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public
# License with the Debian GNU/Linux or Univention distribution in file
# /usr/share/common-licenses/AGPL-3; if not, see
# <https://www.gnu.org/licenses/>.

"""
Logging
=======

This module provides a wrapper for univention.debug
"""

import grp
import logging
import os

import univention.debug as ud

# from univention.management.console.config import ucr


# no exceptions from logging
# otherwise shutdown the server will raise an exception that the logging stream could not be closed
logging.raiseExceptions = 0

#: list of available debugging components
COMPONENTS = (
    ud.MAIN,
    ud.LDAP,
    ud.NETWORK,
    ud.SSL,
    ud.ADMIN,
    ud.MODULE,
    ud.AUTH,
    ud.PARSER,
    ud.LOCALE,
    ud.ACL,
    ud.RESOURCES,
    ud.PROTOCOL,
)

_debug_ready = False
_debug_loglevel = 2
_log_pid = None


# def _reset_debug_loglevel():
#     global _debug_loglevel
#     ucr.load()
#     _debug_loglevel = max(ucr.get_int('umc/server/debug/level', 2), ucr.get_int('umc/module/debug/level', 2))


# _reset_debug_loglevel()


def log_init(filename, log_level=2, log_pid=None):
    """
    Initializes Univention debug.

    :param str filename: The filename just needs to be a relative name. The directory /var/log/univention/ is prepended and the suffix '.log' is appended.
    :param int log_level: log level to use (1-4)
    :param bool log_pid: Prefix log message with process ID
    """
    if not os.path.isabs(filename) and filename not in {"stdout", "stderr"}:
        filename = "/var/log/univention/%s.log" % filename
    fd = ud.init(filename, ud.FLUSH, ud.NO_FUNCTION)
    adm = grp.getgrnam("adm")
    os.fchown(fd.fileno(), 0, adm.gr_gid)
    os.fchmod(fd.fileno(), 0o640)
    log_set_level(log_level)

    global _debug_ready, _log_pid
    _debug_ready = True
    if log_pid is not None:
        _log_pid = log_pid

    return fd


def log_set_level(level=0):
    """
    Sets the log level for all components.

    :param int level: log level to set
    """
    for component in COMPONENTS:
        ud.set_level(component, level)


# def log_reopen():
#     """Reopenes the logfile and reset the current loglevel"""
#     if not _debug_ready:
#         return
#     ud.reopen()
#     _reset_debug_loglevel()
#     log_set_level(_debug_loglevel)


class ILogger(object):
    """
    This class provides a simple interface to access the univention
    debug function for the given component.

    :param int id: id of the component to use
    """

    def __init__(self, id):
        self._id = getattr(ud, id)
        fallbackLoggingFormatter = logging.Formatter(
            "%%(asctime)s.%%(msecs)03d %(component)-11s ( %%(level)-7s ) : %%(message)s"
            % {"component": id},
            "%d.%m.%y %H:%M:%S",
        )
        fallbackLoggingHandler = logging.StreamHandler()
        fallbackLoggingHandler.setFormatter(fallbackLoggingFormatter)
        self._fallbackLogger = logging.getLogger("UMC.%s" % id)
        self._fallbackLogger.setLevel(logging.DEBUG)
        self._fallbackLogger.addHandler(fallbackLoggingHandler)
        self._extras = [
            {"level": "ERROR"},
            {"level": "WARN"},
            {"level": "PROCESS"},
            {"level": "INFO"},
        ]

    def error(self, message):
        """Write a debug message with level ERROR"""
        self.__log(ud.ERROR, message, self._fallbackLogger.error)

    def warn(self, message):
        """Write a debug message with level WARN"""
        self.__log(ud.WARN, message, self._fallbackLogger.warning)

    def process(self, message):
        """Write a debug message with level PROCESS"""
        self.__log(ud.PROCESS, message, self._fallbackLogger.info)

    def info(self, message):
        """Write a debug message with level INFO"""
        self.__log(ud.INFO, message, self._fallbackLogger.debug)

    def debug(self, message):
        """Write a debug message with level INFO"""
        self.__log(ud.ALL, message, self._fallbackLogger.debug)

    def __log(self, level, message, logger):
        if _log_pid:
            message = "%s: %s" % (os.getpid(), message)
        if _debug_ready:
            try:
                ud.debug(self._id, level, message)
            except TypeError:
                ud.debug(self._id, ud.ERROR, "Could not log message %r" % (message,))
        elif _debug_loglevel >= level:
            logger(message, extra=self._extras[level])


CORE = ILogger("MAIN")
NETWORK = ILogger("NETWORK")
CRYPT = ILogger("SSL")
UDM = ILogger("ADMIN")
MODULE = ILogger("MODULE")
AUTH = ILogger("AUTH")
PARSER = ILogger("PARSER")
LOCALE = ILogger("LOCALE")
ACL = ILogger("ACL")
RESOURCES = ILogger("RESOURCES")
PROTOCOL = ILogger("PROTOCOL")
