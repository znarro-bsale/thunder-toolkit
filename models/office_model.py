from typing import List, Dict
from models.multidatabase_model import MultidatabaseModel


class OfficeModel(MultidatabaseModel):
    def get_data_summary(self):
        resp = {}
        try:
            query = """
                  SELECT
                  COUNT(*) AS sucursales,
                  SUM(estado_sucursal = 0) AS sucursales_activas,
                  SUM(estado_sucursal = 1) AS sucursales_inactivas,
                  SUM(es_virtual = 1) AS son_virtuales,
                  SUM(es_bodega = 1) AS son_bodegas
                  FROM sucursal
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


