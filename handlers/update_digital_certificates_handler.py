from handlers.base_handler import BaseHandler
from services.update_digital_certificates_with_related_companies_service import UpdateDigitalCertificatesWithRelatedCompaniesService
from tqdm import tqdm


class UpdateDigitalCertificatesHandler(BaseHandler):
    def __init__(self):
        super().__init__("Update Digital Certificates Handler")

    def update_digital_certificates(self):
        """Obtiene los ATK e ITK de un empresa"""
        try:
            print("Pega el contenido del archivo CSV con encabezados.\nDebe contener las siguientes columnas cpn_id, password y base_64.\nEscribe la palabra 'end' para finalizar la importación:")
            rows = []
            while True:
                row = input()
                if row == 'end':
                    break
                rows.append(row)
                
            update_digital_certificates_service = UpdateDigitalCertificatesWithRelatedCompaniesService(
                self.settings, self.use_proxies)

            data_to_write = []
            for row in tqdm(rows[1:]):
                    row = row.split(',')
                    cpn_id = row[0]
                    password = row[1]
                    base64 = row[2]
                    resp = update_digital_certificates_service.update_chilean_certificates(cpn_id,password,base64)
                    if not resp["success"]:
                        raise ValueError(resp["error"])
                    data_to_write += resp["data"]
            
            data_csv = f"./tmp/{self.env}-update-certificates.csv"
            self._write_csv(data_to_write, data_csv)
            
        except ValueError as error:
            print(error)

    def proccess(self):
        options = [
            {
                "func": self.update_digital_certificates,
                "label": "Actualizar CDs a partir de CSV"
            },
           
        ]
        self._manage_menu_options(options=options)
