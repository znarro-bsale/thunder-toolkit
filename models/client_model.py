from typing import List, Dict
from models.multidatabase_model import MultidatabaseModel


class ClientModel(MultidatabaseModel):
    def get_clients_data_summary(self):
        resp = {}
        try:    
            query = """
                SELECT
                    COUNT(*) AS clientes,
                    COUNT(DISTINCT CASE WHEN c.estado_cliente = 0 THEN c.id_cliente END) AS clientes_activos,
                    COUNT(DISTINCT CASE WHEN c.estado_cliente = 1 THEN c.id_cliente END) AS clientes_inactivos
                FROM cliente c
                WHERE c.id_cliente IS NOT NULL
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


