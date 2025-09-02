from typing import List, Dict
from models.multidatabase_model import MultidatabaseModel


class DevolucionAnulada(MultidatabaseModel):
    def has_annulment_in_the_last_5_days(self):
        resp = {}
        try:    
            query = """
               SELECT count(*) as count FROM devolucion_anulada
               WHERE fecha_anulacion >= DATE_SUB(CURDATE(), INTERVAL 5 DAY);
            """
            r = self._execute_query(query, ())
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["data"] = r["data"][0]

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
