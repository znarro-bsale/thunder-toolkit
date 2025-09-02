from typing import List, Dict
from models.multidatabase_model import MultidatabaseModel


class ProductTypeModel(MultidatabaseModel):
    def get_products_types_data_summary(self):
        resp = {}
        try:    
            query = """
                SELECT
                    COUNT(*) AS tipos_productos,
                    COUNT(DISTINCT CASE WHEN tp.estado_tipo_producto = 0 THEN tp.id_tipo_producto END) AS tipo_productos_activos,
                    COUNT(DISTINCT CASE WHEN tp.estado_tipo_producto = 1 THEN tp.id_tipo_producto END) AS tipo_productos_inactivos
                FROM tipo_producto tp
                WHERE tp.id_tipo_producto IS NOT NULL
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


