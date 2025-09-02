from handlers.base_handler import BaseHandler
from services.expiring_digital_certificate_service import ExpiringDigitalCertificateService


class ExpiringDigitalCertificateHandler(BaseHandler):
    def __init__(self):
        super().__init__("Expiring Digital Certificate Handler")

    def get_expiring_digital_certificates(self):
        """Maneja la generación de un listado de certificados digitales por vencer en Chile"""
        try:
            print("Ingresa la fecha inicial:")
            initial_date = input()
            print("Ingresa la fecha final:")
            end_date = input()
            expiring_digital_certificate_service = ExpiringDigitalCertificateService(
                self.settings, self.use_proxies)
            expiring_digital_certificate = expiring_digital_certificate_service.get_expiring_digital_certificates(
                int(initial_date), int(end_date))
            if not expiring_digital_certificate["success"]:
                raise ValueError(expiring_digital_certificate["error"])

            filename = f"./tmp/{self.env}-certificates.csv"
            self._write_csv(expiring_digital_certificate["data"], filename)
            print(f"Guardado terminado. Revisa {filename}")

        except ValueError as error:
            print(error)

    def proccess(self):
        options = [
            {
                "func": self.get_expiring_digital_certificates,
                "label": "Obtener certificados por rango de fechas"
            },

        ]
        self._manage_menu_options(options=options)
