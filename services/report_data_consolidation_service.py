from typing import List, Dict, Any
from models.sys_config_model import SysConfigModel
from models.sales_document_model import SalesDocumentModel
from models.integration_model import IntegrationModel
from models.data_consolidation_model import DataConsolidationModel
from helpers.database_manager import DatabaseManager


class ReportDataConsolidationService(DatabaseManager):
    def __init__(self, settings: Dict[str, Any], instance_credentials: Dict[str, Any], use_proxies: bool):
        self.cpn_id = instance_credentials["cpn_id"]
        self.access_token = instance_credentials["acs_token"]

        db_connection_data = {
            "host": instance_credentials["cpn_dbase_ip"],
            "database": instance_credentials["cpn_db_name"],
            "user": settings["generalDB"]["user"],
            "password": settings["generalDB"]["password"],
            "port": settings["generalDB"]["port"]
        }

        connection = self._get_connection(db_connection_data)

        self.sys_config_model = SysConfigModel(connection)
        self.sales_document_model = SalesDocumentModel(connection)
        self.integration_model = IntegrationModel(
            settings["apis"], use_proxies)
        self.data_consolidation_model = DataConsolidationModel(
            settings["apis"], use_proxies)

    def set_up_consolidation(self):
        resp = {}
        try:
            use_consolidation_resp = self.sys_config_model.set_use_data_reports_consolidation()
            if not use_consolidation_resp["success"]:
                raise ValueError(use_consolidation_resp["error"])
            # print("use_data_reports_consolidation was set")

            use_bqueues_resp = self.sys_config_model.update_use_bqueues()
            if not use_bqueues_resp["success"]:
                raise ValueError(use_bqueues_resp["error"])
            # print("use_bqueues was updated")

            self._commit()
            set_up_dispatcher_resp = self.integration_model.set_up_dispacther_queues(
                self.access_token, self.cpn_id, ["consolidate_sales_reports"], "document")

            if not set_up_dispatcher_resp["success"]:
                raise ValueError(set_up_dispatcher_resp["error"])
            # print("dispatcher was updated with consolidate_sales_reports queue")

            set_up_dispatcher_resp = self.integration_model.set_up_dispacther_queues(
                self.access_token, self.cpn_id, ["cost_update_sales_reports"], "cost")

            if not set_up_dispatcher_resp["success"]:
                raise ValueError(set_up_dispatcher_resp["error"])
            # print("dispatcher was updated with cost_update_sales_reports queue")

            set_up_dispatcher_resp = self.integration_model.set_up_dispacther_queues(
                self.access_token, self.cpn_id, ["product_update_sales_reports"], "product")

            if not set_up_dispatcher_resp["success"]:
                raise ValueError(set_up_dispatcher_resp["error"])
            # print("dispatcher was updated with product_update_sales_reports queue")

            set_up_dispatcher_resp = self.integration_model.set_up_dispacther_queues(
                self.access_token, self.cpn_id, ["variant_update_sales_reports"], "variant")

            if not set_up_dispatcher_resp["success"]:
                raise ValueError(set_up_dispatcher_resp["error"])
            # print("dispatcher was updated with variant_update_sales_reports queue")

            set_up_dispatcher_resp = self.integration_model.set_up_dispacther_queues(
                self.access_token, self.cpn_id, ["product_type_update_sales_reports"], "product_type")

            if not set_up_dispatcher_resp["success"]:
                raise ValueError(set_up_dispatcher_resp["error"])
            # print("dispatcher was updated with product_type_update_sales_reports queue")

            set_up_dispatcher_resp = self.integration_model.set_up_dispacther_queues(
                self.access_token, self.cpn_id, ["office_update_sales_reports"], "office")

            if not set_up_dispatcher_resp["success"]:
                raise ValueError(set_up_dispatcher_resp["error"])
            # print("dispatcher was updated with office_update_sales_reports queue")

            set_up_dispatcher_resp = self.integration_model.set_up_dispacther_queues(
                self.access_token, self.cpn_id, ["brand_update_sales_reports"], "brand")

            if not set_up_dispatcher_resp["success"]:
                raise ValueError(set_up_dispatcher_resp["error"])
            # print("dispatcher was updated with brand_update_sales_reports queue")

            doc_count_resp = self.sales_document_model.get_documents_count()
            if not doc_count_resp["success"]:
                raise ValueError(doc_count_resp["error"])

            min_id = doc_count_resp["data"]["min"]
            max_id = doc_count_resp["data"]["max"]
            print(f"cpn_id: {self.cpn_id} Documents: Id min - {min_id} Id max - {max_id} ")

            if min_id != None  and min_id < 0:
                min_id = 1 
                
            if min_id != None and max_id != None:
                remove_data_resp = self.data_consolidation_model.remove_data(
                    self.access_token)
                if not remove_data_resp["success"]:
                    raise ValueError(remove_data_resp["error"])
                print(f"cpn_id: {self.cpn_id} data was removed")

                range_size = 50_000
                count = max_id - min_id + 1

                if count < range_size:
                    consolidation_resp = self.data_consolidation_model.consolidate_all_data(
                        self.access_token)
                    if not consolidation_resp["success"]:
                        raise ValueError(consolidation_resp["error"])

                else:
                    n = min_id
                    counter = 0
                    print("Consolidando por rangos")
                    while n <= max_id:
                        start_document_id = n
                        end_document_id = (n - 1) + range_size
                        print(
                            f"cpn_id: {self.cpn_id} Rango  a consolidar: {start_document_id} - {end_document_id} Reintentos: {counter}")

                        while counter <= 3:
                            consolidation_resp = self.data_consolidation_model.consolidate_data_by_range(
                                self.access_token, start_document_id, end_document_id)

                            if not consolidation_resp["success"]:
                                if counter >= 3:
                                    raise ValueError(
                                        consolidation_resp["error"])

                                counter += 1
                                range_size = range_size // 2
                                break
                            else:
                                counter = 0
                                range_size = 50_000
                                n = end_document_id + 1
                                break

                # print("data was consolidated")
            else:
                print(f"cpn_id: {self.cpn_id} - nothing to consolidate")

            resp["success"] = True
        except Exception as e:
            self._rollback()
            resp["success"] = False
            resp["error"] = str(e)
        finally:
            self._close_connection()
        return resp

    def consolidate_data_by_date(self, start_date, end_date):
        resp = {}
        try:
            consolidation_resp = self.data_consolidation_model.consolidate_data_by_date(
                self.access_token, int(start_date), int(end_date))
            if not consolidation_resp["success"]:
                raise ValueError(consolidation_resp["error"])

            resp["success"] = True
        except Exception as e:
            resp["success"] = False
            resp["error"] = str(e)
        return resp

    def disaffiliate_from_data_consolidation(self):
        resp = {}
        try:
            reportv2_variable_count = self.sys_config_model.get_report_v2_variable_count()
            if not reportv2_variable_count["success"]:
                raise ValueError(reportv2_variable_count["error"])
            count = reportv2_variable_count["data"]["count"]
            if count == 0:
                deactivate_variant_resp = self.sys_config_model.update_variant_consolidate_deactivate()
                resp["success"] = True
                resp["data"] = {
                    "row_affected": deactivate_variant_resp["row_affected"]}
                if not deactivate_variant_resp["success"]:
                    raise ValueError(deactivate_variant_resp["error"])
            else:
                raise ValueError(
                    "No se desafilio de la consolidacion, tiene reporte v2")

            self._commit()
        except Exception as e:
            resp["success"] = False
            resp["error"] = str(e)
        finally:
            self._close_connection()
        return resp
