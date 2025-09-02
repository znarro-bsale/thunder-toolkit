
from typing import Dict, Any
from models.company_tracking_model import CompanyTrackingModel
from tqdm import tqdm


class ExpiringDigitalCertificateService:
    def __init__(self, settings: Dict[str, Any], use_proxies: bool) -> None:
        self.tracking_model = CompanyTrackingModel(
            settings["apis"], use_proxies)

    def get_expiring_digital_certificates(self, initial_date: int, end_date: int):
        """Actualiza certificados digitales de forma masiva con empresas asociadas al RUT del RL"""
        resp = {}
        try:
            companies = self.tracking_model.get_all_chilean_companies()
            if not companies["success"]:
                raise ValueError(companies["error"])

            data = []
            for company in tqdm(companies["data"]):
                try:
                    cpn_id = company["cpnId"]
                    companie = self.tracking_model.get_chilean_companie_by_cpn_id(
                        int(cpn_id))
                    expiration_date = companie["data"]["certificate"]["expirationDate"]
                    if expiration_date >= initial_date and expiration_date <= end_date:
                        data.apppend({
                            "cpn": cpn_id,
                            "expiration_date": expiration_date
                        })
                except:
                    next

            resp["data"] = data
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
