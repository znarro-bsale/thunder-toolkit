from handlers.base_handler import BaseHandler
from services.delete_cache_service import DeleteCacheService
from tqdm import tqdm


class DeleteCacheHandler(BaseHandler):
    def __init__(self):
        super().__init__("Delete Cache Handler")

    def delete_by_cpn_id(self):
        cpn_ids_input = input("Ingresa los cpnIds separados por coma: ")
        cpn_ids = [int(cpn_id.strip()) for cpn_id in cpn_ids_input.split(",") if cpn_id.strip().isdigit()]
        
        for cpn_id in tqdm(cpn_ids, desc="Procesando empresas"):
            delete_cache_service = DeleteCacheService(self.settings, self.use_proxies, cpn_id)

            resp_service = delete_cache_service.delete_cache()
            if not resp_service["success"]:
                print(f"Error al borrar cache - cpnId {cpn_id}: {resp_service['error']}")
            else:
                print(f"\nCaché eliminado correctamente para cpnId {cpn_id}")

    def proccess(self):
        options = [
            {
                "func": self.delete_by_cpn_id,
                "label": "Eliminar caché por cpnId"
            }
        ]
        self._manage_menu_options(options=options)
