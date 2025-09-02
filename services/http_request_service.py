from models.integration_model import IntegrationModel
from typing import Dict, Any   
  
class HttpRequestService:
    def __init__(self, settings: Dict[str, Any], instance_credentials: Dict[str, Any], use_proxies: bool):
        self.cpn_id = instance_credentials["cpn_id"]
        self.access_token = instance_credentials["acs_token"]

        self.integration_model = IntegrationModel(
            settings["apis"], use_proxies)

    
    def setup_dispatcher_queues(self, resource, queue):
        resp = {}
        try:
            set_up_dispatcher_resp = self.integration_model.set_up_dispacther_queues(
                self.access_token, self.cpn_id, queue, resource)
            
            if not set_up_dispatcher_resp["success"]:
                raise ValueError(set_up_dispatcher_resp["error"])
            resp["success"] = True
            resp["data"] = set_up_dispatcher_resp["data"]
        except Exception as e:
            resp["success"] = False
            resp["error"] = str(e)
        return resp
