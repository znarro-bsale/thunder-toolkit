from typing import Dict, Callable, Any, List
from services.instance_credentials_service import InstanceCredentialsService
from main import global_env
from main import use_proxies
import traceback
import csv
import json

from services.report_data_consolidation_service import ReportDataConsolidationService


class BaseHandler:    
    def __init__(self, name: str):
        self._load_settings()
        self.name = name
        self.credentials_getters = {
            "cpn_ids": self._get_credentials_for_cpn_id_list,
            "cluster_ips": self._get_credentials_for_cluster_ip_list,
            "quantity": self._get_credentials_for_random_list,
            "all": self._get_credentials_for_all
        }

    def _manage_menu_options(self, options: List[Dict[str, Callable]]):
        """
        Función principal que llama a las funciones para presentar opciones y manejar la entrada del usuario.

        Parameters:
            options (List[Dict[str, Callable]]): Lista de opciones.

        Returns:
            None
        """
        try:
            self._present_options(options)
            self._handle_user_input(options)
        except Exception as e:
            print(f"Error en la ejecución: {e}")

    def _present_options(self, options: List[Dict[str, Callable]]):
        """
        Muestra las opciones disponibles.

        Parameters:
            options (List[Dict[str, Callable]]): Lista de opciones.

        Returns:
            None
        """
        border = "**" + "-"*80 + "**"
        spaces = " "*(40-len(self.name))

        print(f" {border}\n {spaces}{self.name}{spaces} \n {border}")

        print("Opciones: ")
        for index, value in enumerate(options):
            print(f"    {index+1}. {value['label']}")

    def _handle_user_input(self, options: List[Dict[str, Callable]]):
        """
        Maneja la entrada del usuario y ejecuta la opción seleccionada.

        Parameters:
            options (List[Dict[str, Callable]]): Lista de opciones.

        Returns:
            None
        """
        try:
            selected_option = int(input("\nSelecciona una opción: "))
            if selected_option < 1 or selected_option > len(options):
                raise ValueError("La opción no se encuentra en la lista")

            selected_func = options[selected_option-1].get("func")
            if callable(selected_func):
                selected_func()
            else:
                raise ValueError(
                    f"Revisa la función asignada para la opción {selected_option}")

        except ValueError as ve:
            print(f"Opción no válida: {ve}")
        except Exception as e:
            traceback.print_exc()
            print(f"Error: {str(e)}")
        finally:
            print(
                "... Regresando al menú principal.\n\nEscribe 'help' para ver la lista de comandos disponibles.")

    def _get_credentials(self, target: Dict[str, List[Any]] = {}, skip: List[str] = [], **args) -> Dict[str, Any]:
        """
        Obtiene las credenciales según el objetivo especificado en el diccionario.

        Parameters:
            target (Dict[str, List[Any]]): Diccionario que especifica el objetivo y sus parámetros.

        Returns:
            Dict[str, Union[bool, str, Dict[str, Any]]]: Diccionario con el resultado de la operación.
                - 'success': True si la operación fue exitosa, False en caso contrario.
                - 'error': Mensaje de error en caso de fallo.
                - 'data': Datos resultantes en caso de éxito.
        """
        try:
            self.metadatabase_service = InstanceCredentialsService(
                self.settings)
            validation_result = self._validate_target(target)
            if not validation_result["success"]:
                raise ValueError(validation_result["error"])

            getter = self.credentials_getters[validation_result["data"]["grouped_by"]]


            args = {**args, "skip":skip}
            if validation_result["data"]["grouped_by"] == "all":
                return getter(**args)
            else:
                return getter(target.get(validation_result["data"]["grouped_by"]), **args)

        except ValueError as e:
            return {"success": False, "error": str(e)}

    def _validate_target(self, target: Dict[str, List[Any]]) -> Dict[str, Any]:
        resp = {}
        try:
            cpn_ids = target.get("cpn_ids")
            cluster_ips = target.get("cluster_ips")
            quantity = target.get("quantity")

            if sum([bool(cpn_ids), bool(cluster_ips), quantity is not None]) > 1:
                raise ValueError("Must send exactly one of cpn_ids, cluster_ips, or quantity.")

            if not cpn_ids and not cluster_ips and not quantity:
                resp["data"] = {"grouped_by": "all"}
            elif cpn_ids and all(isinstance(id, (int, float)) and id > 0 for id in cpn_ids):
                resp["data"] = {"grouped_by": "cpn_ids"}
            elif cluster_ips and all(isinstance(ip, str) and len(ip) > 0 for ip in cluster_ips):
                resp["data"] = {"grouped_by": "cluster_ips"}
            elif quantity and quantity > 0:
                resp["data"] = {"grouped_by": "quantity"}
            else:
                raise ValueError("Invalid input for cpn_ids or cluster_ips")

            resp["success"] = True
        except Exception as e:
            resp["success"] = False
            resp["error"] = str(e)

        return resp

    def _get_credentials_for_cpn_id_list(self, cpn_ids: List[int], skip: List[str] = []) -> Dict[str, Any]:
        return self.metadatabase_service.get_credentials_for_cpn_id_list(
            cpn_ids, skip)

    def _get_credentials_for_cluster_ip_list(self, cluster_ips: List[str]):
        cpn = {"cpn_id": "", "db_cluster_ip": "someClusterIp", "db_name": "db_1"}
        return [cpn, dict(cpn)]

    def _get_credentials_for_all(self, skip: List[str] = []) -> Dict[str, Any]:
        return self.metadatabase_service.get_credentials_for_all(skip)
    
    def _get_credentials_for_random_list(self, total_empresas: int, cpn_id_list: List[int], skip: List[str] = []) -> Dict[str, Any]:
        print("get_credentials_for_random_list")
        return self.metadatabase_service.get_random_db_access_credentials(total_empresas,cpn_id_list)

    def _write_csv(self, data: List[Dict[str, any]], filename: str) -> None:
        if len(data) == 0:
            print("nothing to save")
            return

        keys = data[0].keys()

        with open(filename, "w", newline="") as csv_file:
            # Use DictWriter for writing dictionaries to CSV
            csv_writer = csv.DictWriter(csv_file, fieldnames=keys)

            csv_writer.writeheader()
            csv_writer.writerows(data)
    
    def _load_settings(self):
        border = "-"*30
        
        global use_proxies
        self.use_proxies = use_proxies == "True"
        
        msg = f"Using proxies: {self.use_proxies}"
        spaces = " "*(15-len(msg))
        print(
            f" {border}\n {spaces}{msg}{spaces} \n {border}")
        
        global global_env
        environments = []
        with open('./config/settings.json', 'r') as file:
            all_settings = json.load(file)
            environments = list(all_settings.keys())

        if global_env not in environments:
            print(
                f"Entorno no reconocido, cargando entorno por defecto '{environments[0]}'...")
            global_env = environments[0]
        
        self.settings = all_settings[global_env]
        self.env = global_env
        
        msg = f"Entorno cargado: [{global_env}]"
        spaces = " "*(15-len(msg))

        print(
            f" {border}\n {spaces}{msg}{spaces} \n {border}")
    
    def _get_valid_selection(self,prompt, options):
        selection = None
        while selection is None:
            print(prompt)
            for index, option in enumerate(options):
                print(f" [{index + 1}] {option}")
            try:
                ind = int(input()) - 1
                if 0 <= ind < len(options):
                    selection = options[ind]
                else:
                    print("Seleccion inválida. Intente nuevamente.\n")
            except ValueError:
                print("Por favor ingrese un número.")
        return selection