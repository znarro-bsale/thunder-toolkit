from typing import List, Dict
from models.multidatabase_model import MultidatabaseModel


class UserModel(MultidatabaseModel):
    def get_sellers_data_summary(self):
        resp = {}
        try:
            query = """
                  SELECT
                    COUNT(DISTINCT CASE WHEN pa.es_perfil_vendedor = 1 AND u.id_usuario <> 1 THEN u.id_usuario END) AS vendedores,
                    COUNT(DISTINCT CASE WHEN pa.es_perfil_vendedor = 1 AND u.id_usuario <> 1 AND u.estado_usuario = 0 THEN u.id_usuario END) AS vendedores_activos,
                    COUNT(DISTINCT CASE WHEN pa.es_perfil_vendedor = 1 AND u.id_usuario <> 1 AND u.estado_usuario = 1 THEN u.id_usuario END) AS vendedores_inactivos,
                    COUNT(DISTINCT CASE WHEN pa.es_perfil_vendedor = 1 AND u.id_usuario <> 1 AND u.es_vendedor_online = 1 THEN u.id_usuario END) AS son_vendedores_online
                  FROM usuario u 
                  LEFT JOIN perfil_usuario pu ON u.id_usuario = pu.id_usuario
                  LEFT JOIN perfil_acceso pa ON pu.id_perfil_acceso = pa.id_perfil_acceso
                  WHERE u.id_usuario IS NOT NULL;
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


