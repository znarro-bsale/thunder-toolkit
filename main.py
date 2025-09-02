import cmd
import os
import json
from typing import IO
global_env = os.environ.get('env', 'development')
use_proxies = os.environ.get('use_proxies', False)

class MyShell(cmd.Cmd):
    intro = """Bienvenido al toolkit de ThunderTeam. Herramientas disponibles:
    1. Borrar cache de instancia(s)
    """
    prompt = "(input) "

    def do_opcion1(self, arg):
        inst = DeleteCacheHandler()
        inst.proccess()

    def do_help(self, arg):
        """Permite obtener un listado de los comandos disponibles"""
        print("1. Borrar cache de instancia(s)")

    def do_exit(self, arg):
        """Sale de la consola"""
        print("¡Hasta luego crack papú gomez!")
        return True

    def default(self, line):
        """Comando ejecutado por defecto cuando no se ingresa una opción disponible"""
        try:
            opcion = int(line)
            method_name = f"do_opcion{opcion}"
            method = getattr(self, method_name)
            method('')
        except (ValueError, AttributeError):
            print(
                "Comando inválido. Escribe 'help' para ver la lista de comandos disponibles.")

    def emptyline(self):
        """Permite controlar cuando no ingresan ninguna opción"""
        pass

if __name__ == '__main__':
 
    from handlers.delete_cache_handler import DeleteCacheHandler
  
    MyShell().cmdloop()


