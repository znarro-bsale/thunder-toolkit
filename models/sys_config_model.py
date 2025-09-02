from typing import List, Dict
from models.multidatabase_model import MultidatabaseModel


class SysConfigModel(MultidatabaseModel):
    def get_report_v2_variable_count(self):
        resp = {}
        try:
            query = """
            SELECT COUNT(*) AS count FROM sys_config WHERE variable IN ('reports_v2_beta', 'reports_v2_gamma', 'reports_v2');
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

    def set_report_v2_variables(self, versions: List[Dict[str, str]]):
        query = """
            INSERT INTO sys_config (variable, valor, descripcion, es_editable, es_empresa, control_html, orden, id_modulo)
            VALUES
              %s
              ;
          """
        placeholders = ', '.join(
            ['(%s, %s, %s, 0, 0, NULL, NULL, 2)' for _ in versions])
        full_query = query % placeholders

        params = []
        for version in versions:
            params.extend([version["variable"],
                          version["valor"], version["description"]])

        return self._execute_query(full_query, tuple(params),False)

    def set_use_data_reports_consolidation(self):
        query = """INSERT INTO `sys_config` (`variable`, `valor`, `descripcion`, `es_editable`, `es_empresa`, `control_html`, `orden`, `id_modulo`)
           VALUES ('use_data_reports_consolidation', '1', NULL, 0, 0, NULL, NULL, 2)
           ON DUPLICATE KEY UPDATE
           `valor` = VALUES(`valor`),
           `descripcion` = VALUES(`descripcion`),
           `es_editable` = VALUES(`es_editable`),
           `es_empresa` = VALUES(`es_empresa`),
           `control_html` = VALUES(`control_html`),
           `orden` = VALUES(`orden`),
           `id_modulo` = VALUES(`id_modulo`);
        """
        return self._execute_query(query, (), False)

    def update_use_bqueues(self):
        query = "UPDATE `sys_config` SET `valor` = '1' WHERE `variable` = 'use_bqueues';"
        return self._execute_query(query, (), False)
    
    def all_report_v2_variables_count(self):
        resp = {}
        try:
            query = """
            SELECT COUNT(*) AS count FROM sys_config WHERE variable IN ('reports_v2_beta', 'reports_v2_gamma', 'reports_v2', 'use_data_reports_consolidation');
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
    
    def has_report_v2_consolidation(self):
        resp = {}
        try:
            query = """
            SELECT 
                CASE 
                WHEN EXISTS (SELECT 1 FROM sys_config WHERE variable = 'use_data_reports_consolidation' AND valor=1) THEN 1
                ELSE 0
                END AS result;
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
    
    
    def update_variant_consolidate_deactivate(self):
        resp = {}
        try:
            query = """
            UPDATE sys_config SET valor=0 WHERE variable = 'use_data_reports_consolidation';
            """
            r = self._execute_query(query, (),False)
            
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["row_affected"] = r["row_affected"]

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
  
    def set_report_v2_details_variables(self, status: int):
        query = """
            INSERT INTO sys_config (variable, valor, descripcion, es_editable, es_empresa, id_modulo)
            VALUES('reports_v2_details', %s, 'sys_config.variable.reports_v2_details', 0, 0, 2);
          """
        return self._execute_query(query=query,params=(int(status),),fetch=False)
    

    def get_report_v2_details_variable(self):
        resp = {}
        try:
            query = """
            SELECT COUNT(*) AS count,
            CASE valor
            WHEN 1 THEN TRUE
            ELSE FALSE
            END AS status 
            FROM sys_config WHERE variable IN ('reports_v2_details') LIMIT 1;
          """
            r = self._execute_query(query, ())
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["success"] = True
            resp["status"] = r["data"][0]["status"]
            resp["data"] = r["data"][0]["count"] > 0  

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
    
    def update_report_v2_details_status(self, status = int):
        resp = {}
        try:
            query = """
            UPDATE sys_config
            SET valor = %s
            WHERE variable IN ('reports_v2_details');
            """
            r = self._execute_query(query=query,params=(int(status),),fetch=False)
            
            if not r["success"]:
                raise ValueError(r["error"])
            resp["success"] = True
            resp["row_affected"] = r["row_affected"]

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
    

    def check_columns_exist(self, table: str, columns: List[str],db_name: str):
        """
        Verifica si las columnas especificadas existen en la tabla dada.
        """
        resp = {}
        try:
            # Consulta SQL para obtener las columnas de la tabla
            query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{db_name}'  # Asegúrate de tener el nombre de la base de datos
            AND TABLE_NAME = '{table}'
            AND COLUMN_NAME IN ({', '.join([f"'{col}'" for col in columns])});
            """
            r = self._execute_query(query, ())
            
            if not r["success"]:
                raise ValueError(r["error"])

            # Obtenemos las columnas existentes
            columnas_existentes = {columna["COLUMN_NAME"] for columna in r["data"]}

            # Verificamos las columnas faltantes
            columnas_faltantes = set(columns) - columnas_existentes

            resp["success"] = True
            resp["columns_found"] = list(columnas_existentes)
            resp["columns_missing"] = list(columnas_faltantes)

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False

        return resp
    
    def set_report_v2_dashboard_variables(self, status: int):
        query = """
            INSERT INTO sys_config (variable, valor, descripcion, es_editable, es_empresa, id_modulo)
            VALUES('reports_v2_dashboard_resumen', %s, 'verifica si el usuario tiene activo el nuevo reports_v2_dashboard', 0, 0, 2)
            ON DUPLICATE KEY UPDATE valor = VALUES(valor);
          """
        return self._execute_query(query=query,params=(int(status),),fetch=False)
    

    def get_report_v2_dashboard_variable(self):
        resp = {}
        try:
            query = """
            SELECT COUNT(*) AS count,
            CASE valor
            WHEN 1 THEN TRUE
            ELSE FALSE
            END AS status 
            FROM sys_config WHERE variable IN ('reports_v2_dashboard_resumen') LIMIT 1;
          """
            r = self._execute_query(query, ())
            if not r["success"]:
                raise ValueError(r["error"])

            resp["success"] = True
            resp["status"] = r["data"][0]["status"]
            resp["data"] = r["data"][0]["count"] > 0  

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
    
    def update_report_v2_dashboard_status(self, status = int):
        resp = {}
        try:
            query = """
            UPDATE sys_config
            SET valor = %s
            WHERE variable IN ('reports_v2_dashboard_resumen');
            """
            r = self._execute_query(query=query,params=(int(status),),fetch=False)
            
            if not r["success"]:
                raise ValueError(r["error"])
            resp["success"] = True
            resp["row_affected"] = r["row_affected"]

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp
    
    def update_margen_porcentd_status(self, status = int):
        resp = {}
        try:
            query = """
            UPDATE sys_config
            SET valor = %s
            WHERE variable = 'margen_porcent';
            """
            r = self._execute_query(query=query,params=(int(status),),fetch=False)
            
            if not r["success"]:
                raise ValueError(r["error"])
            resp["success"] = True
            resp["row_affected"] = r["row_affected"]

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp