from typing import List, Dict, Any
from models.currency_model import CurrencyModel
from models.office_model import OfficeModel
from models.user_model import UserModel
from models.product_type_model import ProductTypeModel
from models.brand_model import BrandModel
from models.client_model import ClientModel
from models.sys_config_model import SysConfigModel
from models.devolucion_anulada_model import DevolucionAnulada
from models.menu_link_model import MenuLinkModel
from helpers.database_manager import DatabaseManager


class InstanceDataService(DatabaseManager):
    def __init__(self, settings: Dict[str, Any], instance_credentials: Dict[str, Any]):
        db_connection_data = {
            "host": instance_credentials["cpn_dbase_ip"],
            "database": instance_credentials["cpn_db_name"],
            "user": settings["generalDB"]["user"],
            "password": settings["generalDB"]["password"],
            "port": settings["generalDB"]["port"]
        }

        connection = self._get_connection(db_connection_data)
        self.currency_model = CurrencyModel(connection)
        self.office_model = OfficeModel(connection)
        self.user_model = UserModel(connection)
        self.product_types_model = ProductTypeModel(connection)
        self.brand_model = BrandModel(connection)
        self.client_model = ClientModel(connection)
        self.sys_config_model = SysConfigModel(connection)
        self.devolucion_anulada_model = DevolucionAnulada(connection)
        self.menu_link_model = MenuLinkModel(connection)
        self._db_name = instance_credentials["cpn_db_name"]

    def get_first_currency_data(self):
        resp = {}
        try:
            currency_resp = self.currency_model.get_first_currency()
            if not currency_resp["success"]:
                raise ValueError(currency_resp["error"])

            resp["data"] = currency_resp["data"]
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp

    def get_office_data_summary(self):
        resp = {}
        try:
            office_resp = self.office_model.get_data_summary()
            if not office_resp["success"]:
                raise ValueError(office_resp["error"])

            resp["data"] = office_resp["data"]
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp

    def get_sellers_data_summary(self):
        resp = {}
        try:
            sellers_resp = self.user_model.get_sellers_data_summary()
            if not sellers_resp["success"]:
                raise ValueError(sellers_resp["error"])

            resp["data"] = sellers_resp["data"]
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp

    def get_products_types_data_summary(self):
        resp = {}
        try:
            sellers_resp = self.product_types_model.get_products_types_data_summary()
            if not sellers_resp["success"]:
                raise ValueError(sellers_resp["error"])

            resp["data"] = sellers_resp["data"]
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp

    def get_brands_data_summary(self):
        resp = {}
        try:
            sellers_resp = self.brand_model.get_brands_data_summary()
            if not sellers_resp["success"]:
                raise ValueError(sellers_resp["error"])

            resp["data"] = sellers_resp["data"]
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp

    def get_clients_data_summary(self):
        resp = {}
        try:
            sellers_resp = self.client_model.get_clients_data_summary()
            if not sellers_resp["success"]:
                raise ValueError(sellers_resp["error"])

            resp["data"] = sellers_resp["data"]
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp

    def get_instance_with_report_v2(self):
        resp = {}
        try:
            sys_config_resp = self.sys_config_model.all_report_v2_variables_count()
            if not sys_config_resp["success"]:
                raise ValueError(sys_config_resp["error"])

            resp["data"] = sys_config_resp["data"]["count"] == 4
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp

    def get_instance_with_consolidation(self):
        resp = {}
        try:
            has_report_v2_consolidation_resp = self.sys_config_model.has_report_v2_consolidation()
            if not has_report_v2_consolidation_resp["success"]:
                raise ValueError(has_report_v2_consolidation_resp["error"])

            get_report_v2_variable_count_resp = self.sys_config_model.get_report_v2_variable_count()
            if not get_report_v2_variable_count_resp["success"]:
                raise ValueError(get_report_v2_variable_count_resp["error"])

            has_consolidation = has_report_v2_consolidation_resp["data"]["result"] == 1
            report_v2_variables_count = get_report_v2_variable_count_resp["data"]["count"]
            resp["data"] = {"has_consolidation": has_consolidation,
                            "report_v2_variables_count": report_v2_variables_count}
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp

    def get_instance_with_has_annulment_in_the_last_5_days(self):
        resp = {}
        try:
            evolucion_anulada_resp = self.devolucion_anulada_model.has_annulment_in_the_last_5_days()
            if not evolucion_anulada_resp["success"]:
                raise ValueError(evolucion_anulada_resp["error"])

            resp["data"] = evolucion_anulada_resp["data"]["count"] > 0
            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp
    
    def get_companies_by_bs_menu_link(self,ml_name, ml_url):
        resp = {}
        try:
            if ml_name == "" or ml_url == "":
                raise ValueError("Menu link name and URL cannot be empty.")

            menu_link_is_found = self.menu_link_model.get_companies_by_bs_menu_link(ml_name, ml_url)
            if not menu_link_is_found["success"]:
                raise ValueError(menu_link_is_found["error"])
            
            resp["success"] = True
            resp["data"] = menu_link_is_found["data"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp
    
    def check_active_sales_report_links(self):
        resp = {}
        try:
            check_active_sales_report = self.menu_link_model.check_active_sales_report_links()
            if not check_active_sales_report["success"]:
                raise ValueError(check_active_sales_report["error"])
            
            resp["success"] = True
            resp["data"] = check_active_sales_report["data"]
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp
    
    def has_sales_detail_activated(self):
        resp = {}
        try:
            check_active_sales_report = self.sys_config_model.get_report_v2_details_variable()
            if not check_active_sales_report["success"]:
                raise ValueError(check_active_sales_report["error"])
            
            resp["success"] = True
            if check_active_sales_report["status"] == 1:
                resp["data"] = True
            else:
                resp["data"] = False
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp

    def check_columns_exist(self, table: str, columns: List[str]):
        resp = {}
        try:
            # Llamamos al modelo para verificar si las columnas existen
            resp = self.sys_config_model.check_columns_exist(table, columns,self._db_name)

        except Exception as error:
            resp["error"] = str(error)
            resp["success"] = False

        return resp