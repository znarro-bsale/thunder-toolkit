from typing import List
from models.multidatabase_model import MultidatabaseModel


class ModuleActionModel(MultidatabaseModel):
    def already_has_module_action(self, module_action: str):
        resp = {}
        try:
            query = """
                SELECT * FROM accion_modulo WHERE nombre_accion_i18n = %s LIMIT 1
            """

            r = self._execute_query(query, (module_action,))
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["data"] = not (len(r["data"]) == 0)
            resp["status"] = True if len(r["data"]) > 0 and r["data"][0]["estado_modulo"] == 0 else False

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False

        return resp

    def insert_report_module_action(self, module_action: str, status: int):
        query = """
                INSERT INTO accion_modulo (nombre_accion_i18n, estado_modulo, id_modulo)
                VALUES (%s, %s, 2);
            """
        return self._execute_query(query=query, params=(
            module_action,int(status),), fetch=False)
    
    def delete_report_module_action(self, module_action: str):
        query = """
                DELETE FROM accion_modulo
                WHERE nombre_accion_i18n = %s;
            """
        return self._execute_query(query=query, params=(
            module_action,), fetch=False)
    
    def update_state_accion_module(self, state_action: int):
        query = """
                UPDATE accion_modulo 
                SET estado_modulo = %s
                WHERE nombre_accion_i18n IN ('accion_modulo.reports_v2.sales_reports.view')
                """
        return self._execute_query(query=query, params=(
            int(state_action),), fetch=False)
