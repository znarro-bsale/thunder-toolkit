from typing import List, Dict, Any
from models.sys_config_model import SysConfigModel
from models.menu_link_model import MenuLinkModel
from models.access_profile_model import AccessProfileModel
from models.module_action_model import ModuleActionModel
from models.menu_services_model import MenuServicesModel
from helpers.database_manager import DatabaseManager
import json


class ReportV2ConfigurationService(DatabaseManager):
    def __init__(self, settings: Dict[str, Any], instance_credentials: Dict[str, Any], use_proxies: bool, versions: Dict[str, Any], views: Dict[str, Any]):
        self.cpn_id = instance_credentials["cpn_id"]
        self.versions = versions
        self.views = views

        db_connection_data = {
            "host": instance_credentials["cpn_dbase_ip"],
            "database": instance_credentials["cpn_db_name"],
            "user": settings["generalDB"]["user"],
            "password": settings["generalDB"]["password"],
            "port": settings["generalDB"]["port"]
        }

        connection = self._get_connection(db_connection_data)

        self.sys_config_model = SysConfigModel(connection)
        self.menu_link_model = MenuLinkModel(connection)
        self.module_action_model = ModuleActionModel(connection)
        self.access_profile_model = AccessProfileModel(connection)
        self.menu_model = MenuServicesModel(
            settings["apis"], use_proxies)

    def set_up_report_v2(self, version_to_configure, view_to_configure):
        resp = {}
        try:
            version = self.versions.get(version_to_configure)
            if not version: 
                 raise ValueError(f"unrecognized version {version}")
             
            view = self.views.get(view_to_configure)
            if not view: 
                 raise ValueError(f"unrecognized version {version}")
            # TODO: ALERTAR SI YA TIENE LIBERADO DETALLE DE VENTA

            already_has_module_resp = self.module_action_model.already_has_module_action("accion_modulo.reports_v2.sales_reports.view")
            if already_has_module_resp['data'] and already_has_module_resp['status']:
                print(f"Tiene liberado detalle de venta. No es posible liberar o cambiar la version de Vista genral en esta opcion")
                resp["success"] = True
                return resp

            already_has_module_resp = self.module_action_model.already_has_module_action(
                view["accion_modulo"])
            if not already_has_module_resp["success"]:
                raise ValueError(already_has_module_resp["error"])
            print(f"has module action: {already_has_module_resp['data']}")

            if already_has_module_resp['data']:
                update_module_url_resp = self.menu_link_model.update_module_url(
                    view["bs_menu_link"]["ml_name"], f"/goto?owner={version['variable']}{view['bs_menu_link']['ml_url']}")

                if not update_module_url_resp["success"]:
                    raise ValueError(update_module_url_resp["error"])

                print(
                    f" > ----- La vista [{view_to_configure}] ya estaba configurada para esta instancia.")
                print(
                    f" > ----- Solo se actualizó a la versión [{version_to_configure}] ")
            else:
                get_variables_count_resp = self.sys_config_model.get_report_v2_variable_count()
                if not get_variables_count_resp["success"]:
                    raise ValueError(get_variables_count_resp["error"])

                if get_variables_count_resp["data"]["count"] == 0:
                    set_variables_resp = self.sys_config_model.set_report_v2_variables(
                        [self.versions["beta"], self.versions["gamma"], self.versions["prod"]])
                    if not set_variables_resp["success"]:
                        raise ValueError(set_variables_resp["error"])

                insert_action_module_resp = self.module_action_model.insert_report_module_action(
                    view["accion_modulo"],0)
                if not insert_action_module_resp["success"]:
                    raise ValueError(insert_action_module_resp["error"])

                insert_report_url_resp = self.menu_link_model.insert_report_module_url(
                    view["bs_menu_link"]["ml_name"], f"/goto?owner={version['variable']}{view['bs_menu_link']['ml_url']}", view["bs_menu_link"]["ml_order"], view["accion_modulo"])
                if not insert_report_url_resp["success"]:
                    raise ValueError(insert_report_url_resp["error"])

                get_profiles_resp = self.access_profile_model.get_access_profiles()
                if not get_profiles_resp["success"]:
                    raise ValueError(get_profiles_resp["error"])

                get_items_count_resp = self.menu_link_model.get_sales_report_module_link_count(
                    view["bs_menu_link"]["ml_name"])
                if not get_items_count_resp["success"]:
                    raise ValueError(get_items_count_resp["error"])

                for profile in get_profiles_resp["data"]:
                    get_count_by_profile_resp = self.menu_link_model.get_sales_report_module_link_count_by_profile_id(
                        profile["id_perfil_acceso"])
                    if not get_count_by_profile_resp["success"]:
                        raise ValueError(get_count_by_profile_resp["error"])

                    if get_count_by_profile_resp["data"]["count"] == get_items_count_resp["data"]["count"]:
                        self.access_profile_model.give_access_to_profile_by_module_action(
                            profile["id_perfil_acceso"], view["accion_modulo"])

            self._commit()
            clear_cache_resp = self.menu_model.clear_cache_by_cpn_id(
                self.cpn_id)
            
            if not clear_cache_resp["success"]:
                print(f"Liberación completada pero no se pudo borrar cache: {clear_cache_resp['error']} - {self.cpn_id}")

            resp["success"] = True
       
        except Exception as error:
            self._rollback()
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp
    
    def set_up_report_v2_sales_reports(self, version_to_configure, view_to_configure):
        resp = {}
        try:
            version = self.versions.get(version_to_configure)
            if not version: 
                 raise ValueError(f"unrecognized version {version}")
             
            view = self.views.get(view_to_configure)
            if not view: 
                 raise ValueError(f"unrecognized version {version}")
            
            # Verificamos si la variable existe
            sys_config_details = self.sys_config_model.get_report_v2_details_variable()
            if not sys_config_details["success"]:
                print(f"No se pudo comprobar la variable reports_v2_details: {sys_config_details['error']}")
                raise ValueError(sys_config_details["error"])
            
            #Si la variable no existe la creamos
            if not sys_config_details['data']:
                # Creamos la variable reports_v2_details en el sys_config con estado activo '1'
                create_sys_config = self.sys_config_model.set_report_v2_details_variables(1)
                print(f"create_sys_config {create_sys_config}")
                if not create_sys_config["success"]:
                    raise ValueError(create_sys_config["error"])
                print(f"Se creo correctamente reports_v2_details: {create_sys_config['row_affected']}")
            elif not sys_config_details["status"]:
                # Activamos la variable reports_v2_details en el sys_config
                activate_sys_config = self.sys_config_model.update_report_v2_details_status(1)
                print(f"activate_sys_config {activate_sys_config}")
                if not activate_sys_config["success"]:
                    raise ValueError(activate_sys_config["error"])
                print(f"Se activo correctamente reports_v2_details: {activate_sys_config['row_affected']}")
            else:
                # Si la variable ya existe y esta activa no hacemos nada
                print(f"reports_v2_details ya está activa")

            # Verificamos si el accion modulo ya existe
            already_has_module_resp = self.module_action_model.already_has_module_action(
                view["accion_modulo"])
            if not already_has_module_resp["success"]:
                print(f"No se pudo comprobar si ya existe el accion_modulo: {view['accion_modulo']}")
                raise ValueError(already_has_module_resp["error"])
            print(f"has module action: {already_has_module_resp['data']}")

            # si ya exciste y esta activo solo actualizamos la version
            if already_has_module_resp["data"] and already_has_module_resp["status"]:
                for munu_link in view['bs_menu_link']:
                    update_module_url_resp = self.menu_link_model.update_module_url(
                        munu_link["ml_name"], f"/goto?owner={version['variable']}{munu_link['ml_url']}")

                    if not update_module_url_resp["success"]:
                        raise ValueError(update_module_url_resp["error"])

                print(
                    f" > ----- La vista [{view_to_configure}] ya estaba configurada para esta instancia.")
                print(
                    f" > ----- Solo se actualizó a la versión [{version_to_configure}] ")
            else:
                # Verificamos si tiene la consolidacion activada
                get_variables_count_resp = self.sys_config_model.get_report_v2_variable_count()
                if not get_variables_count_resp["success"]:
                    raise ValueError(get_variables_count_resp["error"])

                if get_variables_count_resp["data"]["count"] == 0:
                    set_variables_resp = self.sys_config_model.set_report_v2_variables(
                        [self.versions["beta"], self.versions["gamma"], self.versions["prod"]])
                    if not set_variables_resp["success"]:
                        raise ValueError(set_variables_resp["error"])

                # Creamos el nuevo accion modulo 'Reporte Venta'
                if already_has_module_resp["data"] and not already_has_module_resp["status"]:
                    active_action_model =self.module_action_model.update_state_accion_module(0)
                    if not active_action_model['success'] and not active_action_model['row_affected'] != 0:
                        print(f"No se pudo activar el acción módulo: {active_action_model['error']}")
                        raise ValueError(active_action_model['error'])
                    print(f"Se Activa el accion_modulos: {active_action_model['row_affected']}")
                else:
                    insert_action_module_resp = self.module_action_model.insert_report_module_action(view["accion_modulo"], 0)
                    if not insert_action_module_resp["success"] and not insert_action_module_resp['row_affected'] != 0:
                        raise ValueError(insert_action_module_resp["error"])
                    print(f"Se crea el accion_modulo 'Reporte Venta': {insert_action_module_resp['row_affected']}")

                # Crear el bs_menu_link para sales_details y payment_methods
                for insert in view["bs_menu_link_insert"]:
                    insert_report_url_resp = self.menu_link_model.insert_report_module_url(
                        insert["ml_name"], f"/goto?owner={version['variable']}{insert['ml_url']}", insert["ml_order"], view["accion_modulo"])
                    if not insert_report_url_resp["success"] and not insert_report_url_resp['row_affected'] != 0:
                        raise ValueError(insert_report_url_resp["error"])
                    print(f"Se crea el menu_link para {insert['ml_name']}: {insert_report_url_resp['row_affected']}")
                    
                # Modificar el ml_url para Mis Ventas
                update_module_url_link_resp = self.menu_link_model.update_module_url_link(
                        view["update_bs_menu_link_my_sales"]["ml_name"], f"/goto?owner={version['variable']}{view['update_bs_menu_link_my_sales']['ml_url']}")
                if not update_module_url_link_resp["success"] and not update_module_url_link_resp['row_affected'] != 0:
                    raise ValueError(update_module_url_link_resp["error"])
                print(f"Se modificar el ml_url para Mis Ventas: {update_module_url_link_resp['row_affected']}")
                
                # Obtenemos todos los perfiles acctivos
                get_profiles_resp = self.access_profile_model.get_access_profiles()
                if not get_profiles_resp['success']:
                    raise ValueError(get_profiles_resp['error'])

                # Asignar permisos a los perfiles
                print(f"Asignar permisos a los perfiles")
                for profile in get_profiles_resp["data"]:
                    get_count_by_profile_resp = self.menu_link_model.get_sales_report_module_link_count_by_profile_id(
                        profile["id_perfil_acceso"])
                    if not get_count_by_profile_resp["success"]:
                        raise ValueError(get_count_by_profile_resp["error"])

                    if (get_count_by_profile_resp["data"]["count"] > 0):
                        self.access_profile_model.give_access_to_profile_by_module_action(
                            profile["id_perfil_acceso"], view["accion_modulo"])
                
                # Deshabilitar los bs_menu_link de los reportes antiguos
                deactivate_module_url_link = self.menu_link_model.update_state_module_url_link(0)
                if not deactivate_module_url_link['success'] and not deactivate_module_url_link['row_affected'] != 0:
                    print(f"No se pudo desactivar la acción del módulo: {deactivate_module_url_link['error']}")
                    raise ValueError(deactivate_module_url_link['error'])
                print(f"Deshabilitar los bs_menu_link: {deactivate_module_url_link['row_affected']}")

            self._commit()
            clear_cache_resp = self.menu_model.clear_cache_by_cpn_id(
                self.cpn_id)
            if not clear_cache_resp["success"]:
                print(f"Liberación completada pero no se pudo borrar cache: {clear_cache_resp['error']} - {self.cpn_id}")
            else:
                print(f"Liberación completada y cache liberado: {self.cpn_id}")

            resp["success"] = True
       
        except Exception as error:
            self._rollback()
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp

    def delete_reportsv2_sales_detail(self):
        resp = {}
        try:
            # Verificamos si el accion modulo existe
            already_has_sysconfig_resp = self.sys_config_model.get_report_v2_details_variable()
            if not already_has_sysconfig_resp["success"]:
                raise ValueError(already_has_sysconfig_resp["error"])
            print(f"Existe Sys config: {already_has_sysconfig_resp['data']}")

            #Desactivar Reporte de Venta
            deactivate_sysconfig = self.sys_config_model.update_report_v2_details_status(0)
            if not deactivate_sysconfig["success"] and not deactivate_sysconfig['row_affected'] != 0:
                raise ValueError(deactivate_sysconfig["error"])
            print(f"Desactivar variable Sys Config: {deactivate_sysconfig['row_affected']}")


            # Verificamos si el accion modulo existe
            already_has_module_resp = self.module_action_model.already_has_module_action("accion_modulo.reports_v2.sales_reports.view")
            if not already_has_module_resp["success"]:
                raise ValueError(already_has_module_resp["error"])
            print(f"Existe Reporte Venta: {already_has_module_resp['data']}")

            #Desactivar Reporte de Venta
            deactivate_accion_module = self.module_action_model.update_state_accion_module(1)
            if not deactivate_accion_module["success"] and not deactivate_accion_module['row_affected'] != 0:
                raise ValueError(deactivate_accion_module["error"])
            print(f"Desactivar Reporte Venta: {deactivate_accion_module['row_affected']}")

            if deactivate_accion_module["success"] and deactivate_sysconfig["success"]:
                
                #Borrar los bs_menu_link "reports_v2.sales_details", "reports_v2.payment_methods"
                delate_module_url_sales_details = self.menu_link_model.delate_module_url_sales_details()
                if not delate_module_url_sales_details["success"] and not delate_module_url_sales_details['row_affected'] != 0:
                    raise ValueError(delate_module_url_sales_details["error"])
                print(f"Borrar los bs_menu_link - delate_module_url_sales_details: {delate_module_url_sales_details['row_affected']}")

                #Activar los bs_menu_link de los reportes antiguos
                activate_module_url_link = self.menu_link_model.update_state_module_url_link(1)
                if not activate_module_url_link['success'] and not activate_module_url_link['row_affected'] != 0:
                    print(f"No se pudo activar bs_menu_link: {activate_module_url_link['error']}")
                    raise ValueError(activate_module_url_link['error'])
                print(f"Activar los bs_menu_link - activate_module_url_link: {activate_module_url_link['row_affected']}")


                #Modificar el ml_url para Mis Ventas
                update_module_url_link_resp = self.menu_link_model.update_module_url_link(
                        "reports.my_sales", "/goto?owner=bsale&url=/reports/my_sales")
                if not update_module_url_link_resp["success"] and not update_module_url_link_resp['row_affected'] != 0:
                    raise ValueError(update_module_url_link_resp["error"])
                print(f"Mis Ventas - update_module_url_link_resp: {update_module_url_link_resp['row_affected']}")

                print(
                    f" > ----- Se desactivo correctamente 'Detalle de Ventas', 'Formas de Pago' y se modifico la Url de 'Mis Ventas'.")
                print(
                    f" > ----- Se activaraon correctamente los acccion_modulos de los reporstes de ventas antiguos y vista general")
                
            self._commit()
            clear_cache_resp = self.menu_model.clear_cache_by_cpn_id(
                self.cpn_id)
            
            if not clear_cache_resp["success"]:
                print(f"Liberación completada pero no se pudo borrar cache: {clear_cache_resp['error']} - {self.cpn_id}")
            else:
                print(f"Liberación completada y cache liberado: {self.cpn_id}")

            resp["success"] = True
       
        except Exception as error:
            self._rollback()
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp
    
    def add_config_and_action_variables_for_sales_detail_module(self):
        resp = {}
        try:
            # Verificamos si la variable existe
            sys_config_details = self.sys_config_model.get_report_v2_details_variable()
            if not sys_config_details["success"]:
                print(f"No se pudo comprobar la variable reports_v2_details: {sys_config_details['error']}")
                raise ValueError(sys_config_details["error"])
            
            #Si la variable no existe la creamos
            if not sys_config_details['data']:
                # Creamos la variable reports_v2_details en el sys_config con estado desactivado '0'
                create_sys_config = self.sys_config_model.set_report_v2_details_variables(0)
                print(f"create_sys_config {create_sys_config}")
                if not create_sys_config["success"]:
                    raise ValueError(create_sys_config["error"])
                print(f"Se creo correctamente reports_v2_details: {create_sys_config['row_affected']}")
            else:
                print(f"reports_v2_details ya existe")

            # Verificamos si el accion modulo ya existe
            already_has_module_resp = self.module_action_model.already_has_module_action('accion_modulo.reports_v2.sales_reports.view')
            if not already_has_module_resp["success"]:
                raise ValueError(already_has_module_resp["error"])
            
            if not already_has_module_resp['data']:
                 # Creamos el nuevo accion modulo 'Reporte Venta', pasandole el estado desactivado '1'
                insert_action_module_resp = self.module_action_model.insert_report_module_action('accion_modulo.reports_v2.sales_reports.view', 1)
                if not insert_action_module_resp["success"] and not insert_action_module_resp['row_affected'] != 0:
                    raise ValueError(insert_action_module_resp["error"])
                print(f"Se crea el accion_modulo 'Reporte Venta': {insert_action_module_resp['row_affected']}")
            else:
                print(f"El accion_modulo 'Reporte Venta' ya existe")

            
            self._commit()
            clear_cache_resp = self.menu_model.clear_cache_by_cpn_id(
                self.cpn_id)
            
            if not clear_cache_resp["success"]:
                print(f"Liberación completada pero no se pudo borrar cache: {clear_cache_resp['error']} - {self.cpn_id}")
            else:
                print(f"Liberación completada y cache liberado: {self.cpn_id}")

            resp["success"] = True
       
        except Exception as error:
            self._rollback()
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp
    
    def set_up_report_v2_sales_dashboard(self, version_to_configure, view_to_configure):
        resp = {}
        try:
            version = self.versions.get(version_to_configure)
            if not version: 
                 raise ValueError(f"unrecognized version {version}")
             
            view = self.views.get(view_to_configure)
            if not view: 
                 raise ValueError(f"unrecognized version {version}")
            
            # Creamos la variable reports_v2_dashboard_resumen en el sys_config con estado activo '1' o
            # activamos la variable reports_v2_dashboard_resumen en el sys_config
            create_sys_config = self.sys_config_model.set_report_v2_dashboard_variables(1)
            print(f"create_sys_config {create_sys_config}")
            if not create_sys_config["success"]:
                raise ValueError(create_sys_config["error"])
            print(f"Se creo o actualizar correctamente reports_v2_dashboard_resumen: {create_sys_config['row_affected']}")

            # Verificamos si el bs_menu_link ya existe para el menu_v1
            bs_menu_link_resp_v1 = self.menu_link_model.check_active_report_links(view['bs_menu_link']["ml_name"], "bs_mnu_reports")
            if not bs_menu_link_resp_v1["success"]:
                print(f"No se pudo comprobar si ya existe el bs_menu_link: {view['bs_menu_link']['ml_name']}")
                raise ValueError(bs_menu_link_resp_v1["error"])
            
            # Verificamos si el bs_menu_link ya existe para el menu_v2
            bs_menu_link_resp_v2 = self.menu_link_model.check_active_report_links(view['bs_menu_link']["ml_name"], "bs_mnu_reports_v2")
            if not bs_menu_link_resp_v2["success"]:
                print(f"No se pudo comprobar si ya existe el bs_menu_link: {view['bs_menu_link']['ml_name']} en menu_v2")
                raise ValueError(bs_menu_link_resp_v2["error"])
            
            if bs_menu_link_resp_v1["data"]:
                # si ya existe y esta desactivado activamos el bs_menu_link
                if not bs_menu_link_resp_v1["status"] or (bs_menu_link_resp_v2["data"] and not bs_menu_link_resp_v2["status"]):
                    updated_link_state = self.menu_link_model.update_state_module_url_link_dashboard(1)
                    if not updated_link_state["success"]:
                        raise ValueError(updated_link_state["error"])
                    print(f"Se activo el bs_menu_link: {updated_link_state['row_affected']}")
                # si esta activado, solo se modifica la version
                update_module_url_resp = self.menu_link_model.update_module_url(
                view["bs_menu_link"]["ml_name"], f"/goto?owner={version['variable']}{view['bs_menu_link']['ml_url']}")
                if not update_module_url_resp["success"]:
                    raise ValueError(update_module_url_resp["error"])

                print(
                f" > ----- La vista [{view_to_configure}] ya estaba configurada para esta instancia.")
                print(
                f" > ----- Solo se actualizó a la versión [{version_to_configure}] ")
            else:
                # Si no se encuentra el bs_menu_link, lo creamos
                insert_report_url_resp = self.menu_link_model.insert_report_module_url_dashboard(view["bs_menu_link"]["ml_name"], f"/goto?owner={version['variable']}{view['bs_menu_link']['ml_url']}", view["bs_menu_link"]["ml_order"])
                if not insert_report_url_resp["success"]:
                    raise ValueError(insert_report_url_resp["error"])
                print(f"Se creo el bs_menu_link: {insert_report_url_resp['row_affected']}, para {view['bs_menu_link']['ml_name']}, version {version_to_configure}")

                # Verificamos si ya tiene el nuevo menu_v2
                if bs_menu_link_resp_v2["data"]:
                    # Si el bs_menu_link existe en menu_v2 y esta desactivado, lo activamos
                    if not bs_menu_link_resp_v2["status"]:
                        updated_link_state = self.menu_link_model.update_state_module_url_link_dashboard(1)
                        if not updated_link_state["success"]:
                            raise ValueError(updated_link_state["error"])
                        print(f"Se activo el bs_menu_link: {updated_link_state['row_affected']}")
                    # Actualizamos el ml_url del bs_menu_link en menu_v2
                    update_module_url_resp = self.menu_link_model.update_module_url(
                        view["bs_menu_link"]["ml_name"], f"/goto?owner={version['variable']}{view['bs_menu_link']['ml_url']}")
                    if not update_module_url_resp["success"]:
                        raise ValueError(update_module_url_resp["error"])
                    print(f"Se actualizo el ml_url del bs_menu_link: {update_module_url_resp['row_affected']}, para {view['bs_menu_link']['ml_name']}, version {version_to_configure}")
            self._commit()
            clear_cache_resp = self.menu_model.clear_cache_by_cpn_id(self.cpn_id)
            if not clear_cache_resp["success"]:
                print(f"Liberación completada pero no se pudo borrar cache: {clear_cache_resp['error']} - {self.cpn_id}")
            else:
                print(f"Liberación completada y cache liberado: {self.cpn_id}")

            resp["success"] = True
       
        except Exception as error:
            self._rollback()
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp
    
    def deactivate_reports_v2_dashboard(self):
        resp = {}
        try:
            # Verificamos si el sys_config_model existe
            already_has_sysconfig_resp = self.sys_config_model.get_report_v2_dashboard_variable()
            if not already_has_sysconfig_resp["success"]:
                raise ValueError(already_has_sysconfig_resp["error"])
            print(f"Existe Sys config: {already_has_sysconfig_resp['data']}")

            if already_has_sysconfig_resp['data'] and already_has_sysconfig_resp['status']:
                deactivate_sysconfig = self.sys_config_model.update_report_v2_dashboard_status(0)
                if not deactivate_sysconfig["success"] and not deactivate_sysconfig['row_affected'] != 0:
                    raise ValueError(deactivate_sysconfig["error"])
                print(f"Desactivar variable Sys Config: {deactivate_sysconfig['row_affected']}")


            # Verificamos si el bs_menu_link ya existe
            bs_menu_link_resp = self.menu_link_model.check_active_report_links("reports_v2.dashboard")
            if not bs_menu_link_resp["success"]:
                print(f"No se pudo comprobar si ya existe el bs_menu_link: reports_v2.dashboard")
                raise ValueError(bs_menu_link_resp["error"])
            
            if bs_menu_link_resp["data"] and bs_menu_link_resp["status"]:
                updated_link_state = self.menu_link_model.update_state_module_url_link_dashboard(0)
                if not updated_link_state["success"]:
                    raise ValueError(updated_link_state["error"])
                print(f"Se desactiva el bs_menu_link: {updated_link_state['row_affected']}")
            
            print(
                f" > ----- Se desactivo correctamente 'Dashboard'")
                
            self._commit()
            clear_cache_resp = self.menu_model.clear_cache_by_cpn_id(
                self.cpn_id)
            
            if not clear_cache_resp["success"]:
                print(f"Liberación completada pero no se pudo borrar cache: {clear_cache_resp['error']} - {self.cpn_id}")
            else:
                print(f"Liberación completada y cache liberado: {self.cpn_id}")

            resp["success"] = True
       
        except Exception as error:
            self._rollback()
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp

    def update_margin_percentage(self, status: int):
        resp = {}
        try:
            update_margin_resp = self.sys_config_model.update_margen_porcentd_status(status)
            if not update_margin_resp["success"]:
                raise ValueError(update_margin_resp["error"])
            print(f"Se actualizó correctamente el margen porcentual: {update_margin_resp['row_affected']}")
            self._commit()
            resp["success"] = True
        except Exception as error:
            self._rollback()
            resp["error"] = str(error)
            resp["success"] = False
        finally:
            self._close_connection()
        return resp