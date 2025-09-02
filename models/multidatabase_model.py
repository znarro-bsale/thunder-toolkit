import mysql.connector


class MultidatabaseModel:
    # _instance = None  # Variable de clase para almacenar la única instancia

    # def __new__(cls, config):
    #     if not cls._instance:
    #         cls._instance = super(MultidatabaseModel, cls).__new__(cls)
    #         cls._instance.config = config
    #         cls._instance._connect()
    #     return cls._instance

    def __init__(self, connection):
        self.connection = connection

    # def _connect(self):
    #     print("[Abriendo conexión a db]")
    #     try:
    #         connection = mysql.connector.connect(**self.config)
    #         self.connection = connection
    #     except mysql.connector.Error as error:
    #         print("Error al conectarse a la base de datos:", error)

    def _execute_query(self, query, params=None, fetch=True, multi=False):
        resp = {}
        cursor = None

        try:
            if not self.connection.is_connected():
                raise ValueError("Connection is closed")
            cursor = self.connection.cursor()

            total_rows_affected = 0

            if multi:
                results = []
                for result in cursor.execute(query, params, multi=True):
                    if result.with_rows:
                        if fetch:
                            results.append(self._get_results_with_field_names(result.description, result.fetchall()))
                    else:
                        total_rows_affected += result.rowcount
                    
                resp["data"] = results
                resp["row_affected"] = total_rows_affected
            else:
                cursor.execute(query, params)
                if fetch:
                    results = cursor.fetchall()
                    resp["data"] = self._get_results_with_field_names(
                        cursor.description, results)

                resp["success"] = True
                resp["row_affected"] = cursor.rowcount

            resp["success"] = True

        except mysql.connector.Error as error:
            resp["success"] = False
            resp["error"] = str(error)

        except ValueError as error:
            resp["success"] = False
            resp["error"] = str(error)

        finally:
            if cursor:
                cursor.close()

        return resp

    def _get_results_with_field_names(self, description, results):
        field_names = [column[0] for column in description]
        return [dict(zip(field_names, row)) for row in results]
