from typing import List, Dict, Any
from models.menu_v2_model import MenuV2Model
from models.monitor_services_model import MonitorServicesModel
import json


class DeleteCacheService():
    def __init__(self, settings: Dict[str, Any], use_proxies: bool, cpn_id: int):
        self.cpn_id = cpn_id
        self.monitor_model = MonitorServicesModel(settings["apis"], use_proxies)

    def delete_cache(self):
        resp = {}
        try:
            clear_cache_resp = self.monitor_model.clear_monitor_cache_by_cpn_id(self.cpn_id)
            if not clear_cache_resp["success"]:
                print(f"No se pudo borrar cache mediante monitor: {clear_cache_resp['error']} - {self.cpn_id}")

            resp["success"] = True
        except Exception as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp