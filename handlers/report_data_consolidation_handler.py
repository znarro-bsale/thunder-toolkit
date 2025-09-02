from concurrent.futures import ThreadPoolExecutor
from handlers.base_handler import BaseHandler
from services.report_data_consolidation_service import ReportDataConsolidationService
from services.http_request_service import HttpRequestService
from services.report_v2_configuration_service import ReportV2ConfigurationService
from models.redis_model import RedisManager
from datetime import datetime
import json
import asyncio
import time
from tqdm import tqdm

class ReportDataConsolidationHandler(BaseHandler):
    def __init__(self):
        super().__init__("Report Data Consolidation Handler")

        self.redis_manager = RedisManager()
        self.redis_manager._get_connection(self.settings["redis"])

        with open('./config/settings_release_report_v2.json', 'r') as file:
            config_release_report_v2 = json.load(file)
            self.versions = config_release_report_v2.get("release_report_v2_view", {}).get(
                "versions", {})
            self.views = config_release_report_v2.get(
                "release_report_v2_view", {}).get("views", {})
            
        with open('./config/settings_resource_queue.json', 'r') as file:
            config_release_report_v3 = json.load(file)
            self.resource = config_release_report_v3.get("resource_queue", {}).get(
                "rsc", {})
    
    def set_up_consolidation_by_cpn_id(self):
        """Obtiene los ATK e ITK de un empresa"""
        try:
            cpn_id = input("Ingresa cpnId: ")

            resp = self._get_credentials(
                target={"cpn_ids": [int(cpn_id)]}, skip=["atk_itk"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            instance_credentials = resp["data"][0]
            print(instance_credentials)
            report_data_consolidation_service = ReportDataConsolidationService(
                self.settings, instance_credentials, self.use_proxies)

            resp = report_data_consolidation_service.set_up_consolidation()
            if not resp["success"]:
                raise ValueError(resp["error"])

        except ValueError as error:
            print(error)

    def set_up_consolidation_for_many_cpn_id(self, cpn_id_list=[]):
        try:
            if not cpn_id_list:
                print("Ingrese los cpnId separados por coma:")
                cpn_ids = input()
                cpn_id_list = [int(num) for num in cpn_ids.split(',')]

            resp = self._get_credentials(
                target={"cpn_ids": cpn_id_list}, skip=["atk_itk"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            results = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    report_data_consolidation_service = ReportDataConsolidationService(
                        self.settings, instance_credentials, self.use_proxies)
                except Exception as error:
                    results.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = report_data_consolidation_service.set_up_consolidation()
                print("report_data_consolidation_result: ",
                      instance_credentials["cpn_id"], resp["success"])
                if not resp["success"]:
                    results.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue
                results.append(
                    {**instance_credentials, "error": "todo gucci"})

            result_csv = f"./tmp/{self.env}-reportV2-consolidation-results.csv"
            self._write_csv(results, result_csv)

            print(
                f"Guardado terminado. Revisa los resultados en {result_csv}")

        except ValueError as error:
            print(error)

    def set_up_consolidation_by_date(self, cpn_id_list=[], start_date=0, end_date=0):
        try:
            if not cpn_id_list:
                print("Ingrese los cpnId separados por coma:")
                cpn_ids = input()
                cpn_id_list = [int(num) for num in cpn_ids.split(',')]

            if not start_date:
                start_date = input("Ingrese la fecha de inicio (YYYY-MM-DD): ")
                start_date = datetime.strptime(
                    start_date, "%Y-%m-%d").timestamp()

            if not end_date:
                end_date = input("Ingrese la fecha de fin (YYYY-MM-DD): ")
                end_date = datetime.strptime(end_date, "%Y-%m-%d").timestamp()

            target ={"cpn_ids": cpn_id_list}
            if cpn_id_list == "all":
                 target = {}
                 
            resp = self._get_credentials(
                target=target, skip=["atk_itk"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            results = []
            for instance_credentials in resp["data"]:
                # print(instance_credentials)
                try:
                    report_data_consolidation_service = ReportDataConsolidationService(
                        self.settings, instance_credentials, self.use_proxies)
                except Exception as error:
                    results.append(
                        {**instance_credentials, "error": str(error),"success": False})
                    continue

                resp = report_data_consolidation_service.consolidate_data_by_date(
                    start_date, end_date)
                print("set_up_consolidation_by_date: ",
                      instance_credentials["cpn_id"], resp["success"])
                if not resp["success"]:
                    results.append(
                        {**instance_credentials, "error": resp["error"],"success": False})
                    continue
                results.append(
                    {**instance_credentials, "error": "todo gucci","success": True})
            
            for r in results:
                print(r)
                
        except ValueError as error:
            print(error)

    def deactivate_variable_consolidate_cpn_id(self):
        try:
            cpn_id = input("Ingresa cpnId: ")
            resp = self._get_credentials(
                target={"cpn_ids": [int(cpn_id)]}, skip=["atk_itk"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            instance_credentials = resp["data"][0]

            print(instance_credentials)
            report_data_consolidation_service = ReportDataConsolidationService(
                self.settings, instance_credentials, self.use_proxies)

            resp = report_data_consolidation_service.disaffiliate_from_data_consolidation()

            if not resp["success"]:
                raise ValueError(resp["error"])

            print("*"*100)
            print(f" resultado: {resp}")
            print("*"*100)

        except ValueError as error:
            print(error)

    def all_deactivate_variable_consolidate(self):
        try:
            print("Ingrese los cpnId separados por coma:")
            cpn_ids = input()
            cpn_id_list = [int(num) for num in cpn_ids.split(',')]

            resp = self._get_credentials(
                target={"cpn_ids": cpn_id_list}, skip=["atk_itk"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            results = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    report_data_consolidation_service = ReportDataConsolidationService(
                        self.settings, instance_credentials, self.use_proxies)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = report_data_consolidation_service.disaffiliate_from_data_consolidation()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                results.append(
                    {"cpn_id": instance_credentials["cpn_id"], "success": resp["success"], "row_affected": resp["data"]["row_affected"]})

            data_csv = f"./tmp/{self.env}-update-variable-deactivate-consolidacion.csv"
            self._write_csv(results, data_csv)

            errors_csv = f"./tmp/{self.env}-update-variable-deactivate-consolidacion-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)

    def set_reportv2_for_many_cpn_id(self, cpn_id_list=[], version = "", view=""):
        start_time = time.time()
        try:
            if not self.versions.get(version):
                version = self._get_valid_selection("Ingrese la opción de la versión para la cual se configurará:", list(self.versions.keys()))
            
            if not self.views.get(version):
                view = self._get_valid_selection("Ingrese la opción de la pantalla para la cual se configurará:", list(self.views.keys()))
           

            if not cpn_id_list:
                print("Ingrese los cpnId separados por coma:")
                cpn_ids = input()
                cpn_id_list = [int(num) for num in cpn_ids.split(',')]

            resp = self._get_credentials(
                target={"cpn_ids": cpn_id_list}, skip=["atk_itk"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            results = []
            for instance_credentials in resp["data"]:
                resp = self.configure_report_with_data_consolidation(instance_credentials,view,version)
                if not resp["success"]:
                    results.append({**instance_credentials,"error":resp["error"]})
                else:
                    results.append({**instance_credentials,"error":"todo gucci"})

            end_time = time.time()
            duration = end_time - start_time
            print(f"El código tomó {duration:.6f} segundos en ejecutarse.")
            result_csv = f"./tmp/{self.env}-reportV2-liberacion-results.csv"
            self._write_csv(results, result_csv)

            print(
                f"Guardado terminado. Revisa los resultados en {result_csv}")

        except ValueError as error:
            print(error)
 

    def configure_report_with_data_consolidation(self, instance_credentials, view_to_configure, version_to_configure):
        resp = {}
        try:
            cpn_id = instance_credentials.get("cpn_id")
            report_data_consolidation_service = ReportDataConsolidationService(
                    self.settings, instance_credentials, self.use_proxies)
            
            resp = report_data_consolidation_service.set_up_consolidation()
            print("report_data_consolidation_result: ",
                    instance_credentials["cpn_id"], resp["success"])
            if not resp["success"]:
                prev_error = resp["error"]
                restarted_service = ReportDataConsolidationService(
                    self.settings, instance_credentials, self.use_proxies)
                
                print(f"Trying to disaffiliate from consolidation ({cpn_id}) ...")
                resp = restarted_service.disaffiliate_from_data_consolidation()
                if  resp["success"]:
                    error_message = f"{prev_error} Error handled successfully: Desafiliado correctamente"
                else: 
                    print(f"Couldn't disaffiliate from consolidation ({cpn_id})")
                    error_message = f"{prev_error} While handling this error, another one occurred: {resp['error']}"
                    
                raise ValueError(error_message)
            
            report_v2_config_service = ReportV2ConfigurationService(
                    self.settings, instance_credentials, self.use_proxies, self.versions, self.views)
            
            resp = report_v2_config_service.set_up_report_v2(
                version_to_configure, view_to_configure)
            
            print("set_up_report_v2_result: ",
                    instance_credentials["cpn_id"], resp["success"])
            
            if not resp["success"]:
                error_message = f"Consolidación completada. Error liberacion de vista {view_to_configure}: {resp['error']}"
                raise ValueError(error_message)
            
            resp["success"]= True
        except Exception as e:  
            resp["success"] = False
            resp["error"] = str(e)
        finally: 
            return resp
        

    def configure_sales_report(self, instance_credentials, view_to_configure, version_to_configure):
        resp = {}
        try:
            report_v2_config_service = ReportV2ConfigurationService(
                    self.settings, instance_credentials, self.use_proxies, self.versions, self.views)
            
            resp = report_v2_config_service.set_up_report_v2_sales_reports(
                version_to_configure, view_to_configure)
            
            print("set_up_report_v2_result: ",
                    instance_credentials["cpn_id"], resp["success"])
            
            if not resp["success"]:
                error_message = f"Consolidación completada. Error liberacion de vista {view_to_configure}: {resp['error']}"
                raise ValueError(error_message)
            
            resp["success"]= True
        except Exception as e:  
            resp["success"] = False
            resp["error"] = str(e)
        finally: 
            return resp
        
    def configure_dashboard(self, instance_credentials, view_to_configure, version_to_configure):
        resp = {}
        try:
            report_v2_config_service = ReportV2ConfigurationService(
                    self.settings, instance_credentials, self.use_proxies, self.versions, self.views)
            
            resp = report_v2_config_service.set_up_report_v2_sales_dashboard(
                version_to_configure, view_to_configure)
            
            print("set_up_report_v2_result: ",
                    instance_credentials["cpn_id"], resp["success"])
            
            if not resp["success"]:
                error_message = f"Liberacion completada. Error liberacion de vista {view_to_configure}: {resp['error']}"
                raise ValueError(error_message)
            
            resp["success"]= True
        except Exception as e:  
            resp["success"] = False
            resp["error"] = str(e)
        finally: 
            return resp

    async def create_worker(self,name, queue,executor,results, cluster_semaphores):
       while True:
        data = await queue.get()

        instance_credentials = data.get("instance_credentials")
        cpn_id = data.get("instance_credentials").get("cpn_id")
        cluster_name = instance_credentials.get("cpn_dbase_ip") 
        semaphore = cluster_semaphores[cluster_name] # Obtener el semáforo del clúster
        print("#" * 100)
        print(f'semaphore: {cluster_name} {semaphore}')
        print("#" * 100)
        print("%" * 100)
        print(f'{name} has started message: {cpn_id}')
        print("%" * 100)
            
        # Adquirir un permiso del semáforo del clúster antes de procesarlo
        async with semaphore:
            loop = asyncio.get_event_loop()

            function = None
            if data["view_to_configure"] == "sales_reports":
                function = self.configure_sales_report
            elif data["view_to_configure"] == "dashboard":
                function = self.configure_dashboard

            resp = await loop.run_in_executor(executor, function, instance_credentials, data["view_to_configure"], data["version_to_configure"])

            print(f'{name} has completed message: {cpn_id}')

            view_released = False
            consolidated = False

            if resp["success"]:
                view_released = True
                consolidated = True
                error_msg = "todo gucci"
            else:
                error_msg = str(resp["error"])
                consolidated = "Consolidación completada" in error_msg

            results.append({
                "cpn_id": cpn_id,
                "consolidated": consolidated,
                "view_released": view_released,
                "error": error_msg
            })

            
            print("&"*100)
            print(f'Progress: {len(results)}/{queue.maxsize}')
            print("&"*100)
            
            queue.task_done()

    async def create_queue(self,cpn_id_list=[],view="",version=""):
        target ={"cpn_ids": cpn_id_list}
        if cpn_id_list == "all":
           target = {}
           
        resp = self._get_credentials(
                target=target, skip=["atk_itk"])
        
        
      
        if not resp["success"]:
            raise ValueError(resp["error"])
        
        print("INSTANCIAS ENCONTRADAS",len(resp["data"]))
        credentials_list = [c for c in resp["data"] if c.get("cpn_dbase_ip")] 
        print("INSTANCIAS ENCONTRADAS CON CREDENCIALES VALIDAS",len(credentials_list))
        
        queue = asyncio.Queue(len(credentials_list))

        # Crear diccionario de semáforos para cada clúster
        cluster_semaphores = {}
    
        for instance_credentials in credentials_list:
            cluster_name = instance_credentials.get("cpn_dbase_ip") # Usar el nombre completo como identificador
            if cluster_name not in cluster_semaphores:
                cluster_semaphores[cluster_name] = asyncio.Semaphore(5) # Permitir hasta 5 workers por clúster
            
            queue.put_nowait({"instance_credentials":instance_credentials,"view_to_configure":view,"version_to_configure":version, "cluster_name": cluster_name})

        n_workers = 15
        tasks = []
        results = []
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            for i in range(n_workers):
                task = asyncio.create_task(self.create_worker(f'worker-{i+1}', queue, executor,results, cluster_semaphores))
                tasks.append(task)
                
            await queue.join()

            # Cancel our worker tasks.
            for task in tasks:
                task.cancel()
            
            # Wait until all worker tasks are cancelled.
           
            await asyncio.gather(*tasks, return_exceptions=True)

        return results
        

    def set_reportv2_for_many_cpn_id_concurrently(self,cpn_id_list=[], version = "", view=""):
        start_time = time.time()
        
        if not self.versions.get(version):
                version = self._get_valid_selection("Ingrese la opción de la versión para la cual se configurará:", list(self.versions.keys()))
            
        if not self.views.get(view):
                view = self._get_valid_selection("Ingrese la opción de la pantalla para la cual se configurará:", list(self.views.keys()))
                
        if not cpn_id_list:
                list_str = self.redis_manager.get_value("foundation_sales_reports_release_list_ids")
                if list_str == None:
                    print("Ingrese los cpnId separados por coma:")
                    list_str = input()
                    
                cpn_id_list = [int(num) for num in list_str.split(',')]
       
        results = asyncio.run(self.create_queue(cpn_id_list,view,version))
        
        print("Resultados finales: ")
        for result in results:
            print(result)
            
        self._write_csv(results,f"./tmp/{self.env}-reportV2-liberacion-concurrente.csv")
        
        end_time = time.time()
        duration = end_time - start_time
        print(f"El código tomó {duration:.6f} segundos en ejecutarse.")

    def add_queues_to_mongo_integration_api(self, resource=''):
        start_time = time.time()

        try:
            if not self.resource.get(resource):
                resource = self._get_valid_selection("Ingrese el resource a configurar en mongo:", list(self.resource.keys()))

            if resource:
                queue = self.resource.get(resource).get("queue")

            print("Ingrese los cpnId separados por coma (o deje vacío para actualizar todos):")
            cpn_ids = input().strip()

            if cpn_ids:
                cpn_id_list = [int(num.strip()) for num in cpn_ids.split(',') if num.strip()]
                target = {"cpn_ids": cpn_id_list}
            else:
                target = {}

            results = []
            failed_conection_instances = []
            resp = self._get_credentials(
                    target=target, skip=["atk_itk"])
            
            if not resp["success"]:
                raise ValueError(resp["error"])

            for instance_credentials in tqdm(resp["data"], desc="Procesando instancias"):
                try:
                    request_service = HttpRequestService(
                        self.settings, instance_credentials, self.use_proxies)
                except Exception as error:
                    results.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = request_service.setup_dispatcher_queues(resource, queue)
                if not resp["success"]:
                    results.append({**instance_credentials,"error":resp["error"]})
                else:
                    failed_conection_instances.append({**instance_credentials,"error":"todo gucci"})

            end_time = time.time()
            duration = end_time - start_time

            data_csv = f"./tmp/{self.env}-add-queue-mongo.csv"
            self._write_csv(results, data_csv)

            errors_csv = f"./tmp/{self.env}-add-queue-mongo-error.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")
            print(f"El código tomó {duration:.6f} segundos en ejecutarse.")

        except ValueError as error:
            print(error)

    def proccess(self):
        options = [
            {
                "func": self.set_up_consolidation_by_cpn_id,
                "label": "Consolidar una empresa"
            },
            {
                "func": self.set_up_consolidation_for_many_cpn_id,
                "label": "Consolidar varias empresas"
            },
            {
                "func": self.set_up_consolidation_by_date,
                "label": "Consolidar varias empresa por fecha"
            },
            {
                "func": self.deactivate_variable_consolidate_cpn_id,
                "label": "Modificar la variable de configuracion para descativar la consolidacion de una empresa"
            },
            {
                "func": self.all_deactivate_variable_consolidate,
                "label": "Modificar la variable de configuracion para descativar la consolidacion para varias empresas"
            },
            {
                "func":self.set_reportv2_for_many_cpn_id,
                "label":"Liberar reportes v2 para varias empresas"    
            },
            {
                "func":self.set_reportv2_for_many_cpn_id_concurrently,
                "label": "Liberar reportes v2 para varias empresas de forma concurrente"
            },
            {
                "func":self.add_queues_to_mongo_integration_api,
                "label": "Agregar Colas a Mongo mediante integration_api"
            }

        ]
        self._manage_menu_options(options=options)
