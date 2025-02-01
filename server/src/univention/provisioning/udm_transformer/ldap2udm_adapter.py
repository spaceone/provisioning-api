# Callable | NoneSPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import importlib
import logging
from typing import Optional

import univention.admin.uldap
from univention.admin.rest.module import Object
from univention.management.console.log import MODULE
from univention.management.console.modules.udm.udm_ldap import UDM_Module

from .config import UDMTransformerSettings
from .ldap2udm_port import Ldap2Udm

logger = logging.getLogger(__name__)

SUPPORTED_OBJECT_TYPES = [
    "oxmail/accessprofile",
    "oxmail/functional_account",
    "oxmail/oxcontext",
    "oxresources/oxresources",
    "portals/all",
    "portals/announcement",
    "portals/category",
    "portals/entry",
    "portals/folder",
    "portals/portal",
    "portals/announcement",
    "portals/category",
    "portals/entry",
    "portals/folder",
    "portals/portal",
    "users/user",
    "groups/group",
]

UDM_MODULES_RELOAD_TRIGGER = {
    "settings/extended_attribute",
    "settings/extended_options",
    "settings/udm_hook",
    "settings/udm_module",
    "settings/udm_syntax",
}


class ModuleNotFound(Exception):
    pass


class Ldap2UdmAdapter(Ldap2Udm, univention.admin.uldap.access):
    def __init__(self, settings: Optional[UDMTransformerSettings] = None):
        Ldap2Udm.__init__(self, settings)
        univention.admin.uldap.access.__init__(
            self,
            host=settings.ldap_host,
            port=settings.ldap_port,
            start_tls=2 if settings.ldap_tls_mode.lower() == "on" else 0,
            base=settings.ldap_base_dn,
            binddn=settings.ldap_bind_dn,
            bindpw=settings.ldap_bind_pw,
        )
        self._my_ldap_position = univention.admin.uldap.position(settings.ldap_base_dn)
        self.ldap_publisher_name = settings.ldap_publisher_name

    @staticmethod
    def reload_udm_if_required(obj: dict) -> None:
        if obj.get("objectType") not in UDM_MODULES_RELOAD_TRIGGER:
            return
        logger.info("Reload of UDM modules triggered by change of %r object.", obj["objectType"])
        importlib.reload(univention.management.console.modules.udm.udm_ldap)

    def _get_module(self, object_type) -> UDM_Module:
        module = UDM_Module(object_type, ldap_connection=self, ldap_position=self._my_ldap_position)
        if not module or not module.module:
            raise ModuleNotFound
        return module

    def ldap_to_udm(self, entry: dict) -> dict:
        object_types = entry.get("univentionObjectType", [])
        if not isinstance(object_types, list) or len(object_types) < 1:
            MODULE.warn("ReadControl response is missing `univentionObjectType`!")
            return {}

        object_type = object_types[0].decode("utf-8")
        try:
            module = self._get_module(object_type)
            module_obj = module.module.object(
                co=None,
                lo=self,
                position=self._my_ldap_position,
                dn=entry["entryDN"][0],
                superordinate=None,
                attributes=entry,
            )
            module_obj.open()
            return Object.get_representation(module, module_obj, ["*"], self, False)
        except ModuleNotFound:
            MODULE.error("ReadControl response has object type %r, but the module was not found!" % object_type)
            return {}
        except Exception:
            if object_type in SUPPORTED_OBJECT_TYPES:
                raise
            logger.exception(
                "Ignoring failed transformation of unsupported UDM type %r. Object: %r", object_type, entry
            )
