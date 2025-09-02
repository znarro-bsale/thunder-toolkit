from typing import Dict
from helpers.api_client import ApiClient


class CompanyTrackingModel(ApiClient):
    def __init__(self, api_list, use_proxies):
        super().__init__(api_list["trackingAPI"], use_proxies)

    def get_all_chilean_companies(self):
        return self._call(endpoint="/v1/companies.json?is_active=true&in_production=true")

    def get_chilean_companie_by_cpn_id(self, cpn_id: int):
        return self._call(endpoint=f"/v1/companies/{cpn_id}.json?expand=[certificate]")

    def update_chilean_certificate_in_mongodb(self, cpn_id: int, data: Dict):
        """Actualiza un certificado digital chileno en MongoDB"""
        return self._call(endpoint=f"/v1/companies/{cpn_id}.json", payload=data, method="PUT")
