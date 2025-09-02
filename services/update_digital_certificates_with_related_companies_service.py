
from typing import List, Dict, Any
from models.bsp_service_model import BspServiceModel
from models.company_tracking_model import CompanyTrackingModel
from models.drone_model import DroneModel


class UpdateDigitalCertificatesWithRelatedCompaniesService:
    def __init__(self, settings: Dict[str, Any], use_proxies: bool) -> None:
        self.bsp_model = BspServiceModel(
            settings["apis"], use_proxies)
        self.tracking_model = CompanyTrackingModel(
            settings["apis"], use_proxies)
        self.drone_model = DroneModel(
            settings["apis"], use_proxies)

    def update_chilean_certificates(self, cpn_id: int, password, base64):
        """Actualiza certificados digitales de forma masiva con empresas asociadas al RUT del RL"""
        resp = {}
        try:
            user_support = 48
            companies_updated = []
            related_companies_resp = self.bsp_model.get_related_companies_based_on_legal_agent_code_by_cpn_id(
                int(cpn_id))
            if not related_companies_resp["success"]:
                companies_updated.append({
                    "cpn_id": cpn_id,
                    "comment": "data not found",
                    "mongodb_updated": False,
                    "bsp_updated": False,
                    "drone_updated": False
                })
                next
            related_companies = related_companies_resp["data"]["data"]["relatedCpnsBasedOnLegalAgentCode"]
            for related_company in related_companies:
                if related_company["country"] == "CL":
                    temp_cpn_id = related_company["cpnId"]
                    certificate_uploaded = self.upload_chilean_certificates(
                        user_support, int(temp_cpn_id), password, base64)
                    if not certificate_uploaded["success"]:
                        companies_updated.append({
                              "cpn_id": cpn_id,
                              "comment": certificate_uploaded["error"],
                              "mongodb_updated": False,
                              "bsp_updated": False,
                              "drone_updated": False
                          })
                   
                    companies_updated.append(certificate_uploaded["data"])
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    def upload_chilean_certificates(self, user_id: int, cpn_id: int, password: str, base64: str):
        """Carga el certificado por empresa en las BD"""
        resp = {}
        try:
            name = "cert-%s" % (cpn_id)
            size = (3 * len(base64)) / 4 - 1
            content_type = 'application/x-pkcs12'
            json_for_mongo_db = {
                "certificate": {
                    "userId": user_id,
                    "cpnId": cpn_id,
                    "pass": password,
                    "name": name,
                    "size": size,
                    "contentType": content_type,
                    "base64": base64,
                }
            }
            json_fo_maria_db = {
                "uploadToS3": True,
                "userId": user_id,
                "cpnId": cpn_id,
                "pass": password,
                "name": name,
                "size": size,
                "contentType": content_type,
                "base64": base64,
            }
            mongodb_cert_updated = self.tracking_model.update_chilean_certificate_in_mongodb(
                cpn_id, json_for_mongo_db)
            bsp_cert_updated = self.bsp_model.add_chilean_certificate_in_bsp(
                json_fo_maria_db)
            drone_cert_updated = self.drone_model.add_chilean_certificate_in_drone(
                json_fo_maria_db)
            resp["data"] = {
                              "cpn_id": cpn_id,
                              "comment": "",
                              "mongodb_updated": mongodb_cert_updated["success"],
                              "bsp_updated": bsp_cert_updated["success"],
                              "drone_updated":  drone_cert_updated["success"]
                          }
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
