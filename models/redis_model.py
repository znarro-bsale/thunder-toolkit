import redis

class RedisManager:
    def __init__(self):
        self.client = None

    def _get_connection(self, config):
        print("[Abriendo conexión a Redis]")
        try:
            self.client = redis.Redis(
                # host=config["host"], 
                # port=config["port"], 
                # db=config["db"], 
                # password=config["password"]
                **config
            )
            if self.client.ping():
                print("Conexión exitosa a Redis")
            return self.client
        except redis.ConnectionError as error:
            print(f"Error al conectarse a Redis: {error}")
            raise ValueError(f"Error connecting to Redis: {error}")
        except Exception as error:
            print(f"Ocurrió un error: {error}")
            raise

    def _close_connection(self):
        if self.client:
            print("[Cerrando conexión a Redis]")
            self.client.close()
            self.client = None

    def set_value(self, key, value, ex=None):
        if self.client:
            self.client.set(key, value, ex=ex)

    def get_value(self, key):
        if self.client:
            value = self.client.get(key)
            return value.decode() if value else None
        
    def delete_value(self, key):
        if self.client:
            value = self.client.delete(key)
            return value.decode() if value else None