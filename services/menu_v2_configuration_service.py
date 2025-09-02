from typing import List, Dict, Any
from models.menu_v2_model import MenuV2Model
from models.monitor_services_model import MonitorServicesModel
from helpers.database_manager import DatabaseManager
import json


class MenuV2ConfigurationService(DatabaseManager):
    def __init__(self, settings: Dict[str, Any], instance_credentials: Dict[str, Any], use_proxies: bool, versions: Dict[str, Any]):
        self.cpn_id = instance_credentials["cpn_id"]
        self.versions = versions
        self.bway_database_manager = DatabaseManager()

        db_connection_data = {
            "host": instance_credentials["cpn_dbase_ip"],
            "database": instance_credentials["cpn_db_name"],
            "user": settings["generalDB"]["user"],
            "password": settings["generalDB"]["password"],
            "port": settings["generalDB"]["port"]
        }

        connection = self._get_connection(db_connection_data)
        bway_connection = self.bway_database_manager._get_connection(settings["bway"])

        self.menu_v2_model = MenuV2Model(connection)
        self.monitor_model = MonitorServicesModel(settings["apis"], use_proxies)
        self.menu_v2_bway_model = MenuV2Model(bway_connection)

    def set_up_menu_v2(self, version_to_configure):
        resp = {}
        try:
            version = self.versions.get(version_to_configure, {}).get("value")
            if not version: 
                 raise ValueError(f"unrecognized version {version}")
            
            if version == 1:
                # Solo actualiza la tabla tbw_companies
                resp = self.menu_v2_bway_model.set_menu_version(version, self.cpn_id)
                if not resp["success"]:
                    raise ValueError(resp["error"])
                
                self.bway_database_manager._commit()
            else:
                # hacer las consultas
                resp_bs_menu = self.menu_v2_model.set_bs_menu_items()
                print(f"resp_bs_menu {resp_bs_menu}")
                if not resp_bs_menu["success"]:
                    raise ValueError(resp_bs_menu["error"])

                resp_temp_table = self.menu_v2_model.set_temporary_table()
                print(f"resp_temp_table {resp_temp_table}")
                if not resp_temp_table["success"]:
                    raise ValueError(resp_temp_table["error"])
                
                resp_clients = self.menu_v2_model.set_menu_clients()
                print(f"resp_clients {resp_clients}")
                if not resp_clients["success"]:
                    raise ValueError(resp_clients["error"])
                
                resp_products = self.menu_v2_model.set_menu_products()
                print(f"resp_products {resp_products}")
                if not resp_products["success"]:
                    raise ValueError(resp_products["error"])

                resp_documents = self.menu_v2_model.set_menu_documents()
                print(f"resp_documents {resp_documents}")
                if not resp_documents["success"]:
                    raise ValueError(resp_documents["error"])

                resp_settings = self.menu_v2_model.set_menu_settings()
                print(f"resp_settings {resp_settings}")
                if not resp_settings["success"]:
                    raise ValueError(resp_settings["error"])

                resp_reports = self.menu_v2_model.set_menu_reports()
                print(f"resp_reports {resp_reports}")
                if not resp_reports["success"]:
                    raise ValueError(resp_reports["error"])

                resp_online_store = self.menu_v2_model.set_menu_online_store()
                print(f"resp_online_store {resp_online_store}")
                if not resp_online_store["success"]:
                    raise ValueError(resp_online_store["error"])
                
                resp_online_store_mp = self.menu_v2_model.set_menu_online_store_mp()
                print(f"resp_online_store_mp {resp_online_store_mp}")
                if not resp_online_store_mp["success"]:
                    raise ValueError(resp_online_store_mp["error"])

                resp_main = self.menu_v2_model.set_menu_main()
                print(f"resp_main {resp_main}")
                if not resp_main["success"]:
                    raise ValueError(resp_main["error"])

                resp_delete_temp_table = self.menu_v2_model.clear_temporary_table()
                print(f"resp_delete_temp_table {resp_delete_temp_table}")
                if not resp_delete_temp_table["success"]:
                    raise ValueError(resp_delete_temp_table["error"])

                resp_landing_url = self.menu_v2_model.set_bsale_landing_page()
                print(f"resp_landing_url {resp_landing_url}")
                if not resp_landing_url["success"]:
                    raise ValueError(resp_landing_url["error"])
                
                resp_bsale_help_url = self.menu_v2_model.update_bsale_help_url()
                print(f"resp_bsale_help_url {resp_bsale_help_url}")
                if not resp_bsale_help_url["success"]:
                    raise ValueError(resp_bsale_help_url["error"])
                
                resp_set_menu_version = self.menu_v2_bway_model.set_menu_version(version, self.cpn_id)
                print(f"resp_set_menu_version {resp_set_menu_version}")
                if not resp_set_menu_version["success"]:
                    raise ValueError(resp_set_menu_version["error"])

                # actualizar el product_type
                # resp_update_product_type = self.menu_v2_model.update_product_type()
                # print(f"resp_update_product_type {resp_update_product_type}")
                # if not resp_update_product_type["success"]:
                #     raise ValueError(resp_update_product_type["error"])

                # actualizar el stock por product_type
                # resp_update_stock_by_product_type = self.menu_v2_model.update_stock_by_product_type()
                # print(f"resp_update_stock_by_product_type {resp_update_stock_by_product_type}")
                # if not resp_update_stock_by_product_type["success"]:
                #     raise ValueError(resp_update_stock_by_product_type["error"])

                # actualizar el accion_modulo_pos_libro_mensual
                # resp_update_accion_modulo_pos_libro_mensual = self.menu_v2_model.update_accion_modulo_pos_libro_mensual()
                # print(f"resp_update_accion_modulo_pos_libro_mensual {resp_update_accion_modulo_pos_libro_mensual}")
                # if not resp_update_accion_modulo_pos_libro_mensual["success"]:
                #     raise ValueError(resp_update_accion_modulo_pos_libro_mensual["error"])

                # actualizar el update_accion_modulo_pos_online_orders
                # resp_update_accion_modulo_pos_online_orders = self.menu_v2_model.update_accion_modulo_pos_online_orders()
                # print(f"resp_update_accion_modulo_pos_online_orders {resp_update_accion_modulo_pos_online_orders}")
                # if not resp_update_accion_modulo_pos_online_orders["success"]:
                #     raise ValueError(resp_update_accion_modulo_pos_online_orders["error"])

                self._commit()
                self.bway_database_manager._commit()

            clear_cache_resp = self.monitor_model.clear_monitor_cache_by_cpn_id(self.cpn_id)
            if not clear_cache_resp["success"]:
                print(f"Liberación completada pero no se pudo borrar cache mediante monitor: {clear_cache_resp['error']} - {self.cpn_id}")

            resp["success"] = True
        except Exception as error:
            self._rollback()
            self.bway_database_manager._rollback()
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
            self.bway_database_manager._close_connection()
        return resp