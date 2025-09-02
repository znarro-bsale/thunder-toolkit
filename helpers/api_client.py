import requests
from requests.auth import HTTPProxyAuth
# import socks
# import socket
import json


class ApiClient:
    def __init__(self, base_url: str, use_proxies: bool):
        self.use_proxies = use_proxies
        self.base_url = base_url

    def _call(self, endpoint, method='GET', headers={}, payload=None):
        resp = {}
        
        try:
            response = None
            url = f"{self.base_url}{endpoint}"
            # print(url)

            data = None
            if payload != None:
                data = json.dumps(payload)

            # print("data: ", data)
            headers.update({"Content-Type": "application/json"})

            if self.use_proxies:
                proxies = self._make_request_through_ssh_tunnel()
                response =requests.request(method.upper(),url, headers=headers, data=data, proxies=proxies,timeout=(10, 120))
            else:
                response = requests.request(method.upper(), url, headers=headers, data=data)
            
            response.raise_for_status()
            if response.text:
                resp["data"] = response.json()
                resp["success"] = True
            else:
                resp["data"] = None
                resp["success"] = True
        except requests.Timeout:
            resp["error"] = (
                f"La solicitud a {url} ha excedido el tiempo de espera.")
            resp["success"] = False
        except requests.RequestException as e:
            details = ""
            try: 
                resp = response.json()
                if 'errors' in resp:
                    details = resp["errors"]
            except:
                details =""
            resp["error"] = f"Error en la solicitud a {url}: {e}. {details}"
            resp["success"] = False
        except Exception as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    def _make_request_through_ssh_tunnel(self, proxy_host='127.0.0.1', proxy_port=8080, proxy_auth=None):
        # Configuración del proxy SOCKS
        # socks.set_default_proxy(socks.SOCKS5, proxy_host, proxy_port)
        # socket.socket = socks.socksocket

        # Configuración de autenticación del proxy si se proporciona
        # auth = None
        # if proxy_auth:
        #     auth = HTTPProxyAuth(
        #         proxy_auth['username'], proxy_auth['password'])

        # Realiza la solicitud a través del túnel SSH
        proxies = {'http': f'http://{proxy_host}:{proxy_port}',
                   'https': f'http://{proxy_host}:{proxy_port}'}

        return proxies
