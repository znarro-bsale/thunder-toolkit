from typing import Dict
from helpers.api_client import ApiClient


class BspServiceModel(ApiClient):
    def __init__(self, api_list, use_proxies):
        super().__init__(api_list["bspServiceAPI"], use_proxies)

    def get_related_companies_based_on_legal_agent_code_by_cpn_id(self, cpn_id: int):
        return self._call(endpoint=f"/v2/companies/{cpn_id}.json?expand=[related_cpns_based_on_legal_agent_code]")

    def add_chilean_certificate_in_bsp(self, data: Dict):
        """Agrega un certificado digital chileno en BSP"""
        return self._call(endpoint="/v1/certificates.json", method="POST", payload=data)
