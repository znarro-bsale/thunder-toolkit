from handlers.base_handler import BaseHandler
from services.report_v2_configuration_service import ReportV2ConfigurationService
import json
from tqdm import tqdm


class SetReportV2ConfigurationHandler(BaseHandler):
    def __init__(self):
        super().__init__("Set Report V2 Configuration Handler")
        with open('./config/settings_release_report_v2.json', 'r') as file:
            config_release_report_v2 = json.load(file)
            self.versions = config_release_report_v2.get("release_report_v2_view", {}).get(
                "versions", {})
            self.views = config_release_report_v2.get(
                "release_report_v2_view", {}).get("views", {})

    def set_configuration_by_cpn_id(self):
        try:
            version = self._get_valid_selection("Ingrese la opción de la versión para la cual se configurará:", list(self.versions.keys()))
            view = self._get_valid_selection("Ingrese la opción de la pantalla para la cual se configurará:", list(self.views.keys()))

            cpn_id = input("Ingresa cpnId: ")

            resp = self._get_credentials(
                target={"cpn_ids": [int(cpn_id)]}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            instance_credentials = resp["data"][0]
            report_v2_config_service = ReportV2ConfigurationService(
                self.settings, instance_credentials, self.use_proxies, self.versions, self.views)

            if view == "sales_overview":
                resp = report_v2_config_service.set_up_report_v2(
                    version, view)
            elif view == "sales_reports":
                resp = report_v2_config_service.set_up_report_v2_sales_reports(version, view)
            else:
                resp = report_v2_config_service.set_up_report_v2_sales_dashboard(version, view)
            
            if not resp["success"]:
                raise ValueError(resp["error"])

        except ValueError as error:
            print(error)

    def set_configuration_many_cpn_ids(self):
        try:
            version = self._get_valid_selection("Ingrese la opción de la versión para la cual se configurará:", list(self.versions.keys()))
            view = self._get_valid_selection("Ingrese la opción de la pantalla para la cual se configurará:", list(self.views.keys()))

            print("Ingrese los cpnId separados por coma:")
            cpn_ids = input()
            cpn_id_list = [int(num) for num in cpn_ids.split(',')]

            resp = self._get_credentials(
                target={"cpn_ids": cpn_id_list}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])
            
            results = []
            for instance_credentials in tqdm(resp["data"], desc="Procesando instancias"):
                print(instance_credentials)
                try: 
                    report_v2_config_service = ReportV2ConfigurationService(
                    self.settings, instance_credentials, self.use_proxies, self.versions, self.views)
                except Exception as error:
                    results.append(
                        {**instance_credentials, "error": str(error)})
                    continue
                if view == "sales_overview":
                    resp = report_v2_config_service.set_up_report_v2()
                elif view == "sales_reports":
                    resp = report_v2_config_service.set_up_report_v2_sales_reports(version, view)
                else:
                    resp = report_v2_config_service.set_up_report_v2_sales_dashboard(version, view)
                
                if not resp["success"]:
                    results.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue
                results.append(
                        {**instance_credentials, "error": "todo gucci"})
                

            result_csv = f"./tmp/{self.env}-reportV2-configuration-results.csv"
            self._write_csv(results, result_csv)
            
            print(
            f"Guardado terminado. Revisa los resultados en {result_csv}")

                
        except ValueError as error:
            print(error)
        
    def set_desactive_sales_details_by_cpn_ids(self):
        try:

            print("¡¡¡¡¡Esta por desactivar 'Reportes de Ventas'-(Detalle de Ventas, Formas de Pago, Mis Ventas)!!!!!")
            print("Ingrese los cpnId separados por coma:")
            cpn_ids = input()
            cpn_id_list = [int(num) for num in cpn_ids.split(',')]
            target = {"cpn_ids": cpn_id_list}

            resp = self._get_credentials(
                target, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])
            
            results = []
            for instance_credentials in tqdm(resp["data"], desc="Procesando instancias"):
                report_v2_config_service = ReportV2ConfigurationService(
                    self.settings, instance_credentials, self.use_proxies, self.versions, self.views)

                resp = report_v2_config_service.delete_reportsv2_sales_detail()

                if not resp["success"]:
                    results.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue
                results.append(
                        {**instance_credentials, "error": "todo gucci"})
                
            
            result_csv = f"./tmp/{self.env}-reportV2-configuration-results.csv"
            self._write_csv(results, result_csv)
            
            print(
            f"Guardado terminado. Revisa los resultados en {result_csv}")

        except ValueError as error:
            print(error)
    
    def set_desactive_sales_dashboard_by_cpn_ids(self):
        try:

            print("¡¡¡¡¡Esta por desactivar 'Reportes Dashboard'!!!!!")
            print("Ingrese los cpnId separados por coma:")
            cpn_ids = input()
            cpn_id_list = [int(num) for num in cpn_ids.split(',')]
            target = {"cpn_ids": cpn_id_list}

            resp = self._get_credentials(
                target, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])
            
            results = []
            for instance_credentials in tqdm(resp["data"], desc="Procesando instancias"):
                report_v2_config_service = ReportV2ConfigurationService(
                    self.settings, instance_credentials, self.use_proxies, self.versions, self.views)

                resp = report_v2_config_service.deactivate_reports_v2_dashboard()

                if not resp["success"]:
                    results.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue
                results.append(
                        {**instance_credentials, "error": "todo gucci"})
                
            
            result_csv = f"./tmp/{self.env}-reportV2-configuration-results.csv"
            self._write_csv(results, result_csv)
            
            print(
            f"Guardado terminado. Revisa los resultados en {result_csv}")

        except ValueError as error:
            print(error)

    
    def set_configuration_sys_config_reports(self, cpn_id_list = []):
        try:
            target = {}
            if cpn_id_list != "all":
                print("Ingrese los cpnId separados por coma:")
                cpn_ids = input()
                cpn_id_list = [int(num) for num in cpn_ids.split(',')]
                target = {"cpn_ids": cpn_id_list}

            resp = self._get_credentials(
                target, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])
            
            results = []
            for instance_credentials in tqdm(resp["data"], desc="Procesando instancias"):
                print(instance_credentials)
                try: 
                    report_v2_config_service = ReportV2ConfigurationService(
                    self.settings, instance_credentials, self.use_proxies,self.versions, self.views)
                except Exception as error:
                    results.append(
                        {**instance_credentials, "error": str(error)})
                    continue
          
                resp = report_v2_config_service.add_config_and_action_variables_for_sales_detail_module()
                
                
                if not resp["success"]:
                    results.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue
                results.append(
                        {**instance_credentials, "error": "todo gucci"})
                

            result_csv = f"./tmp/{self.env}-reportV2-configuration-results.csv"
            self._write_csv(results, result_csv)
            
            print(
            f"Guardado terminado. Revisa los resultados en {result_csv}") 

        except ValueError as error:
            print(error)

    def update_margin_percentage(self, cpn_id_list=[], status = int):
        try:
            print("¡¡¡¡¡Esta por actualizar el porcentaje de margen!!!!!")
            
            target = {}
            if cpn_id_list != "all":
                print("Ingrese los cpnId separados por coma:")
                cpn_ids = input()
                cpn_id_list = [int(num) for num in cpn_ids.split(',')]
                target = {"cpn_ids": cpn_id_list}

            if status not in [0, 1]:
                status = int(input("Ingrese el estado del porcentaje de margen (0(Desactivado) o 1(Activado)): "))

            resp = self._get_credentials(
                target, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])
            
            results = []
            for instance_credentials in tqdm(resp["data"], desc="Procesando instancias"):
                print(instance_credentials)
                try: 
                    report_v2_config_service = ReportV2ConfigurationService(
                    self.settings, instance_credentials, self.use_proxies,self.versions, self.views)
                except Exception as error:
                    results.append(
                        {**instance_credentials, "error": str(error)})
                    continue
          
                resp = report_v2_config_service.update_margin_percentage(status)
                
                
                if not resp["success"]:
                    results.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue
                results.append(
                        {**instance_credentials, "error": "todo gucci"})
                

            result_csv = f"./tmp/{self.env}-reportV2-configuration-results.csv"
            self._write_csv(results, result_csv)
            
            print(
            f"Guardado terminado. Revisa los resultados en {result_csv}") 
            
        except ValueError as error:
            print(error)

    def proccess(self):
        options = [
            {
                "func": self.set_configuration_by_cpn_id,
                "label": "Configurar una empresa"
            },
            {
                "func": self.set_configuration_many_cpn_ids,
                "label": "Configurar varias empresas"
            },
            {
                "func": self.set_desactive_sales_details_by_cpn_ids,
                "label": "Desactivar a una empresa 'Reportes de Ventas'"
            },
            {
                "func": self.set_configuration_sys_config_reports,
                "label": "Configurar variable de configuración y acción para el módulo de Detalle de Ventas"
            },
            {
                "func": self.set_desactive_sales_dashboard_by_cpn_ids,
                "label": "Desactivar a una empresa el Dashboard de Reportes V2"
            },
            {
                "func": self.update_margin_percentage,
                "label": "Actualizar porcentaje de margen"
            }

        ]
        self._manage_menu_options(options=options)
