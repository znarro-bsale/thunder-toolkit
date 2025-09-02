from helpers.api_client import ApiClient

class MonitorServicesModel(ApiClient):
    def __init__(self, api_list, use_proxies):
        super().__init__(api_list["monitorAPI"], use_proxies)
        
    def _handle_request(self, endpoint, method, headers={}, payload=None):
        resp = {}
        try:
            response = self._call(
                endpoint=endpoint, headers=headers, method=method, payload=payload)
            
            if not response["success"]:
                raise ValueError(response["error"])

            resp["data"] = response["data"]
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    def clear_monitor_cache_by_cpn_id(self, cpn_id):
        return self._handle_request(endpoint=f"/v1/operations/{cpn_id}/cache.json", method="DELETE", headers={'access_token': 'f6eebcb3a9e1ed9c698f8e58a60c2226f0aaf0fa'})
