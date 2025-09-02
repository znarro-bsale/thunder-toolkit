from handlers.base_handler import BaseHandler
from services.menu_v2_configuration_service import MenuV2ConfigurationService
import json
from tqdm import tqdm


class SetMenuV2ConfigurationHandler(BaseHandler):
    def __init__(self):
        super().__init__("Set Menu V2 Configuration Handler")
        with open('./config/settings_menu_v2.json', 'r') as file:
            config_menu_v2 = json.load(file)
            self.versions = config_menu_v2.get("release_menu_v2", {}).get("versions", {})
            self.modes = config_menu_v2.get("release_menu_v2", {}).get("modes", {})

    def set_configuration_by_cpn_id(self):
        try:
            mode = self.modes[self._get_valid_selection("Seleccione el modo de configuración:", list(self.modes.keys()))].get("value")
            version = self._get_valid_selection("Ingrese la opción de la versión para la cual se configurará:", list(self.versions.keys()))

            if mode == "single":
                cpn_ids = [int(input("Ingresa cpnId: "))]
            elif mode == "multiple":
                cpn_ids_input = input("Ingresa los cpnIds separados por coma: ")
                cpn_ids = [int(cpn_id.strip()) for cpn_id in cpn_ids_input.split(",") if cpn_id.strip().isdigit()]
            else:
                raise ValueError("Modo no reconocido")
            
            for cpn_id in tqdm(cpn_ids, desc="Procesando empresas"):
                resp_credentials = self._get_credentials(target={"cpn_ids": [cpn_id]}, skip=["atk_itk", "acs_token"])
                if not resp_credentials["success"]:
                    print(f"Error en cpnId {cpn_id}: {resp_credentials['error']}")
                    continue

                instance_credentials = resp_credentials["data"][0]
                menu_v2_config_service = MenuV2ConfigurationService(self.settings, instance_credentials, self.use_proxies, self.versions)

                resp_service = menu_v2_config_service.set_up_menu_v2(version)
                if not resp_service["success"]:
                    print(f"Error al configurar cpnId {cpn_id}: {resp_service['error']}")
        except ValueError as error:
            print(error)

    def proccess(self):
        options = [
            {
                "func": self.set_configuration_by_cpn_id,
                "label": "Configurar empresa(s)"
            }
        ]
        self._manage_menu_options(options=options)
