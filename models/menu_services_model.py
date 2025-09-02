from helpers.api_client import ApiClient


class MenuServicesModel(ApiClient):
    def __init__(self, api_list, use_proxies):
        super().__init__(api_list["menuAPI"], use_proxies)
        
    def _handle_request(self, endpoint, method, headers={}, payload=None):
        resp = {}
        try:
            response = self._call(
                endpoint=endpoint, headers=headers, method=method, payload=payload)
            if not response["success"]:
                raise ValueError(response["error"])

            api_response = response["data"]
            if 'errors' in api_response:
                error_message = api_response['errors']
                raise ValueError(
                    f"Error: {error_message} - code: {api_response['code']}")

            resp["data"] = api_response['data']
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
      
    def clear_cache_by_cpn_id(self, cpn_id):
        return self._handle_request(endpoint=f"/v1/menu/cpn/cache/all.json?cpn={cpn_id}", method="DELETE")
