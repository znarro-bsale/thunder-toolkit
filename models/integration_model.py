from helpers.api_client import ApiClient


class IntegrationModel(ApiClient):
    def __init__(self, api_list, use_proxies):
        super().__init__(api_list["integratorAPI"], use_proxies)

    def set_up_dispacther_queues(self, access_token, cpn_id, queues,resource_type):
        headers = {"access_token": access_token}
        payload = {
            "cpn": cpn_id,
            "rsc": resource_type,
            "inf": "bsale",
            "dsp": queues
        }

        return self._call(endpoint="/v1/dispatcher.json", headers=headers, payload=payload, method="POST")
