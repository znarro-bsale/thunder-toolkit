# foundation-toolkit

## toolkit en local

Se necesita tener instalado los siguientes

- python:
  ```
  pip install tqdm
  pip install mysql-connector-python
  pip install requests
  pip install redis
  pip install pandas
  ```
- ruby (get atk atk)
  Asegurarse de tener ruby 1.8.0 en el sistema

- ejecución:
  Se tiene que enviar la variable "env", la cual hace referencia a uno lo los ambientes existentes en el archivo settings.json,
  en el caso de que se envie un entorno que no esta definido; por defecto se configurará con el entorno de "development".
  ```
  env=development use_proxies=False python3 main.py
  env=production use_proxies=True python3 main.py
  ```

## toolkit en Docker

Ejecutar:

1. Construcción de imagen:

   - Para entorno de desarrollo :

   ```
       docker build -t foundation-toolkit --build-arg ENV=development -f docker/Dockerfile .
   ```

   - Para entorno de producciòn :

   ```
       docker build -t foundation-toolkit --build-arg ENV=production -f docker/Dockerfile .
   ```

2. Ejecutar contenedor: -i (modo interactivo) -i(asigna un seudoterminal (TTY))

```
docker run -i -t foundation-toolkit:latest
```
