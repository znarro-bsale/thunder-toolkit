from typing import Dict
from helpers.api_client import ApiClient


class DroneModel(ApiClient):
    def __init__(self, api_list, use_proxies):
        super().__init__(api_list["droneAPI"], use_proxies)

    def add_chilean_certificate_in_drone(self, data: Dict):
        """Agrega un certificado digital chileno en Drone"""
        return self._call(endpoint="/v1/certificates.json", method="POST", payload=data)
