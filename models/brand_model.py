from typing import List, Dict
from models.multidatabase_model import MultidatabaseModel


class BrandModel(MultidatabaseModel):
    def get_brands_data_summary(self):
        resp = {}
        try:    
            query = """
                SELECT
                    COUNT(*) AS marcas,
                    COUNT(DISTINCT CASE WHEN b.br_status = 0 THEN b.br_id END) AS marcas_activos,
                    COUNT(DISTINCT CASE WHEN b.br_status = 1 THEN b.br_id END) AS marcas_inactivos
                FROM bs_brand b
                WHERE b.br_id IS NOT NULL
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


