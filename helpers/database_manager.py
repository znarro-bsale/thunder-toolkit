
import mysql.connector


class DatabaseManager:
    def __init__(self):
        self.connection = None

    def _get_connection(self, config):
        print("[Abriendo conexión a db]")
        try:
            connection = mysql.connector.connect(**config)
            self.connection = connection

            cursor = self.connection.cursor()
            cursor.execute("SET wait_timeout = 120")
            self.connection.commit()
            cursor.close()
            
            print("idle time 120s")
            return self.connection
        except mysql.connector.Error as error:
            print("Error al conectarse a la base de datos:", error)
            raise ValueError(f"Error connecting to db: {error}")
        except Exception as error:
            print(f"Ocurrio un error {error}")

    def _close_connection(self):
        if self.connection.is_connected():
            print("[Cerrando conexion a db]")
            self.connection.close()

    def _commit(self):
        print("[Confirmando cambios]")
        self.connection.commit()

    def _rollback(self):
        if self.connection.is_connected():
            print("[Deshaciendo cambios]")
            self.connection.rollback()
        else:
            print("[Conexion cerrada, rollback no disponible]")
        
