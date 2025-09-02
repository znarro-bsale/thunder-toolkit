from handlers.base_handler import BaseHandler

class GetAtkItkHandler(BaseHandler):
    def __init__(self):
        super().__init__("ATK - ITK Handler ")

    def get_atk_itk_for_all_instances(self):
        """Obtiene los ATK e ITK de todas las empresas"""
        try:
            resp = self._get_credentials(
                target={}, skip=["db_access","acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])
            filename = f"./tmp/{self.env}-atk_itk.csv"
            self._write_csv(resp["data"], filename)
            print(f"Guardado terminado. Revisa {filename}")
        except ValueError as error:
            print(error)

    def get_atk_itk_by_cpn_id(self):
        """Obtiene los ATK e ITK de un empresa"""
        try:
            cpn_id = input("Ingresa cpnId: ")

            resp = self._get_credentials(
                target={"cpn_ids": [int(cpn_id)]}, skip=["db_access","acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            print(f"ATK e ITK de la empresa con cpnId {cpn_id}")
            response = resp["data"][0]
            print(f"ATK: {response['atk']}")
            print(f"ITK: {response['itk']}")
        except ValueError as error:
            print(error)

    

    def process(self):
        options = [
            {
                "func": self.get_atk_itk_by_cpn_id,
                "label": "Obtener atk e itk para una empresa"
            },
            {
                "func": self.get_atk_itk_for_all_instances,
                "label": "Obtener atk e itk para una todas las empresas"
            }
        ]
        self._manage_menu_options(options=options)
