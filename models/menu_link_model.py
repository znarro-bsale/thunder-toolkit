from typing import List
from models.multidatabase_model import MultidatabaseModel


class MenuLinkModel(MultidatabaseModel):
    def update_module_url(self, module_name: str, module_url: str):
        query = """
                UPDATE bs_menu_link SET ml_url = %s WHERE ml_name = %s
                """
        return self._execute_query(query=query, params=(
            module_url, module_name,), fetch=False)

    def insert_report_module_url(self, module_name: str, module_url: str, module_order: int, module_action: str):
        query = """
                INSERT INTO bs_menu_link (ml_name, ml_active, ml_asociate, ml_url, m_id, ml_is_dropdown, id_accion_modulo, ml_order)
                VALUES (%s, 1, 1, %s, 1, 0, (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = %s), %s);
                """
        return self._execute_query(query=query, params=(
            module_name, module_url, module_action, module_order), fetch=False)

    def get_sales_report_module_link_count(self, module_name):
        resp = {}
        try:
            query = """
                        SELECT COUNT(*) AS count FROM bs_menu_link ml
                        WHERE ml.m_id = 1 
                        AND ml.ml_active = 1 
                        AND ml.ml_asociate = 1 
                        AND ml.ml_name NOT IN (%s)
                        """
            r = self._execute_query(query, (module_name,))
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["data"] = r["data"][0]

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
            
        return resp

    def get_sales_report_module_link_count_by_profile_id(self, profile_id):
        resp = {}
        try:
            query = """
                        SELECT COUNT(*) as count FROM bs_menu_link ml
                        INNER JOIN perfil_accion pa ON ml.id_accion_modulo = pa.id_accion_modulo
                        WHERE pa.id_perfil_acceso = %s
                        AND ml.m_id = 1
                        AND ml.ml_active = 1
                        AND ml.ml_asociate = 1
                        AND ml_name IN ("reports.by_office", "reports.by_salesman", "reports.by_client", "reports.by_product","reports.by_product_type" )
                        """
            r = self._execute_query(query, (profile_id,))
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["data"] = r["data"][0]

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
            
        return resp
    
    def update_module_url_link(self, module_name: str, module_url: str):
        query = """
                UPDATE bs_menu_link 
                SET ml_url = %s 
                WHERE ml_name = %s
                """
        return self._execute_query(query=query, params=(
            module_url, module_name,), fetch=False)
    
    def update_module_url_id_accion(self, module_name: str, module_action: str):
        query = """
                UPDATE bs_menu_link 
                SET id_accion_modulo = (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = %s  LIMIT 1)
                WHERE ml_name = %s
                """
        return self._execute_query(query=query, params=(
            module_action, module_name,), fetch=False)
    
    def delate_module_url_sales_details(self):
        query = """
                DELETE FROM bs_menu_link
                WHERE ml_name IN ("reports_v2.sales_details", "reports_v2.payment_methods")
                """
        return self._execute_query(query=query, params=(), fetch=False)
    
    def update_state_module_url_link(self, state_action: int):
        query = """
                UPDATE bs_menu_link 
                SET ml_active = %s
                WHERE ml_name IN ('reports.by_office','reports.by_salesman','reports.by_client','reports.by_product','reports.by_product_type')
                """
        return self._execute_query(query=query, params=(
            int(state_action),), fetch=False)
    
    def get_companies_by_bs_menu_link(self, ml_name, ml_url):
        resp = {}
        try:
            query = """
            SELECT COUNT(*) AS count
            FROM bs_menu_link
            WHERE ml_name = %s AND ml_url LIKE %s;
          """
            # Agregar % al ml_url para la búsqueda con LIKE
            ml_url = f"%{ml_url}%"
            
            r = self._execute_query(query, (ml_name, ml_url))
            if not r["success"]:
                raise ValueError(r["error"])
            
            resp["success"] = True
            if r["data"][0]["count"] > 0:
                resp["data"] = True
            else:
                resp["data"] = False

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
    
    def check_active_sales_report_links(self):
        resp = {}
        try:
            query = """
            SELECT COUNT(*) AS count
            FROM bs_menu_link
            WHERE ml_name = "reports_v2.sales_details" 
            AND ml_name IN ('reports.by_office', 'reports.by_product') 
            AND ml_active = 1;
          """
            r = self._execute_query(query, ())
            if not r["success"]:
                raise ValueError(r["error"])
            
            resp["success"] = True
            if r["data"][0]["count"] > 0:
                resp["data"] = True
            else:
                resp["data"] = False

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
    
    def insert_report_module_url_dashboard(self, module_name: str, module_url: str, module_order: int):
        query = """
                INSERT INTO bs_menu_link (ml_name, ml_active, ml_asociate, ml_url, m_id, ml_is_dropdown, id_accion_modulo, ml_order)
                VALUES (%s, 1, 1, %s, 1, 0, 0, %s);
                """
        return self._execute_query(query=query, params=(
            module_name, module_url, module_order), fetch=False)
    
    def update_state_module_url_link_dashboard(self, state_action: int):
        query = """
                UPDATE bs_menu_link 
                SET ml_active = %s
                WHERE ml_name IN ('reports_v2.dashboard')
                """
        return self._execute_query(query=query, params=(
            int(state_action),), fetch=False)

    def check_active_report_links(self, ml_name=str, name_version_menu=""):
        resp = {}
        try:
            # Fragmentos de la consulta
            join_clause = ""
            where_extra = ""

            # Si se pasa el name_version_menu, agregamos el JOIN y el filtro adicional
            if name_version_menu:
                join_clause = "INNER JOIN bs_menu bm ON bml.m_id = bm.m_id"
                where_extra = "AND bm.m_name = %s"

            query = f"""
                SELECT COUNT(*) AS count,
                CASE bml.ml_active
                    WHEN 1 THEN TRUE
                    ELSE FALSE
                END AS status
                FROM bs_menu_link bml
                {join_clause}
                WHERE bml.ml_name = %s
                {where_extra};
            """

            # Definimos los parámetros en orden
            params = (str(ml_name),) if not name_version_menu else (str(ml_name), str(name_version_menu))

            r = self._execute_query(query=query, params=params)
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            if r["data"][0]["count"] > 0:
                resp["data"] = True
                resp["status"] = r["data"][0]["status"]
            else:
                resp["data"] = False

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False

        return resp

