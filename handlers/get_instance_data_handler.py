from handlers.base_handler import BaseHandler
from services.instance_data_service import InstanceDataService
import pandas as pd


class GetInstanceDataHandler(BaseHandler):
    def __init__(self):
        super().__init__("Get Instance Data Handler")

    def get_currency_for_all_instances(self):
        try:
            resp = self._get_credentials(
                target={}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.get_first_currency_data()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {"cpn_id": instance_credentials["cpn_id"], **resp["data"]})

            data_csv = f"./tmp/{self.env}-currency.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-currency-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)

    def get_currency_by_cpn_id(self):
        try:
            cpn_id = input("Ingresa cpnId: ")

            resp = self._get_credentials(
                target={"cpn_ids": [int(cpn_id)]}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            instance_credentials = resp["data"][0]
            instance_data_service = InstanceDataService(
                self.settings, instance_credentials)

            resp = instance_data_service.get_first_currency_data()
            if not resp["success"]:
                raise ValueError(resp["error"])

            print(f"Moneda de la empresa con cpnId {cpn_id}")
            print(resp["data"])

        except ValueError as error:
            print(error)

    def get_office_summary_by_cpn_id(self):
        try:
            cpn_id = input("Ingresa cpnId: ")

            resp = self._get_credentials(
                target={"cpn_ids": [int(cpn_id)]}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            instance_credentials = resp["data"][0]
            instance_data_service = InstanceDataService(
                self.settings, instance_credentials)

            resp = instance_data_service.get_office_data_summary()
            if not resp["success"]:
                raise ValueError(resp["error"])

            print(f"Oficinas de la empresa con cpnId {cpn_id}")
            print(resp["data"])

        except ValueError as error:
            print(error)

    def get_offices_summary_for_all_instances(self):
        try:
            resp = self._get_credentials(
                target={}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.get_office_data_summary()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {"cpn_id": instance_credentials["cpn_id"], **resp["data"]})

            data_csv = f"./tmp/{self.env}-office.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-office-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)

    def get_sellers_summary_by_cpn_id(self):
        try:
            cpn_id = input("Ingresa cpnId: ")

            resp = self._get_credentials(
                target={"cpn_ids": [int(cpn_id)]}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            if len(resp["data"])==0:
                raise ValueError(f"no se obtuvieron credenciales para {cpn_id}")
            
            instance_credentials = resp["data"][0]
            instance_data_service = InstanceDataService(
                self.settings, instance_credentials)

            resp = instance_data_service.get_sellers_data_summary()
            if not resp["success"]:
                raise ValueError(resp["error"])

            print(f"Vendedores de la empresa con cpnId {cpn_id}")
            print(resp["data"])

        except ValueError as error:
            print(error)

    def get_sellers_summary_for_all_instances(self):
        try:
            resp = self._get_credentials(
                target={}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.get_sellers_data_summary()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {"cpn_id": instance_credentials["cpn_id"], **resp["data"]})

            data_csv = f"./tmp/{self.env}-sellers.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-sellers-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)

    def get_products_types_summary_by_cpn_id(self):
        try:
            cpn_id = input("Ingresa cpnId: ")

            resp = self._get_credentials(
                target={"cpn_ids": [int(cpn_id)]}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            if len(resp["data"])==0:
                raise ValueError(f"no se obtuvieron credenciales para {cpn_id}")
            
            instance_credentials = resp["data"][0]
            instance_data_service = InstanceDataService(
                self.settings, instance_credentials)

            resp = instance_data_service.get_products_types_data_summary()
            if not resp["success"]:
                raise ValueError(resp["error"])

            print(f"Tipos de productos de la empresa con cpnId {cpn_id}")
            print(resp["data"])

        except ValueError as error:
            print(error)

    def get_products_types_summary_for_all_instances(self):
        try:
            resp = self._get_credentials(
                target={}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.get_products_types_data_summary()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {"cpn_id": instance_credentials["cpn_id"], **resp["data"]})

            data_csv = f"./tmp/{self.env}-products-types.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-products-types-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)

    def get_brands_summary_by_cpn_id(self):
        try:
            cpn_id = input("Ingresa cpnId: ")

            resp = self._get_credentials(
                target={"cpn_ids": [int(cpn_id)]}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            if len(resp["data"])==0:
                raise ValueError(f"no se obtuvieron credenciales para {cpn_id}")
            
            instance_credentials = resp["data"][0]
            instance_data_service = InstanceDataService(
                self.settings, instance_credentials)

            resp = instance_data_service.get_brands_data_summary()
            if not resp["success"]:
                raise ValueError(resp["error"])

            print(f"Marcas de la empresa con cpnId {cpn_id}")
            print(resp["data"])

        except ValueError as error:
            print(error)

    def get_brands_summary_for_all_instances(self):
        try:
            resp = self._get_credentials(
                target={}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.get_brands_data_summary()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {"cpn_id": instance_credentials["cpn_id"], **resp["data"]})

            data_csv = f"./tmp/{self.env}-brands.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-brands-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)

    def get_clients_summary_by_cpn_id(self):
        try:
            cpn_id = input("Ingresa cpnId: ")

            resp = self._get_credentials(
                target={"cpn_ids": [int(cpn_id)]}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            if len(resp["data"])==0:
                raise ValueError(f"no se obtuvieron credenciales para {cpn_id}")
            
            instance_credentials = resp["data"][0]
            instance_data_service = InstanceDataService(
                self.settings, instance_credentials)

            resp = instance_data_service.get_clients_data_summary()
            if not resp["success"]:
                raise ValueError(resp["error"])

            print(f"Clientes de la empresa con cpnId {cpn_id}")
            print(resp["data"])

        except ValueError as error:
            print(error)

    def get_clients_summary_for_all_instances(self):
        try:
            resp = self._get_credentials(
                target={}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.get_clients_data_summary()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {"cpn_id": instance_credentials["cpn_id"], **resp["data"]})

            data_csv = f"./tmp/{self.env}-clients.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-clients-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)
    
    def get_instance_with_all_report_v2(self):
        try:
            resp = self._get_credentials(
                target={}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.get_instance_with_report_v2()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {"cpn_id": instance_credentials["cpn_id"], "has_report_v2":resp["data"]})

            data_csv = f"./tmp/{self.env}-report_v2.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-report_v2-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)

    def get_instance_with_report_v2_cpn_id(self):
        try:
            cpn_id = input("Ingresa cpnId: ")

            resp = self._get_credentials(
                target={"cpn_ids": [int(cpn_id)]}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            instance_credentials = resp["data"][0]
            instance_data_service = InstanceDataService(
                self.settings, instance_credentials)

            resp = instance_data_service.get_instance_with_report_v2()
            if not resp["success"]:
                raise ValueError(resp["error"])

            print(f"Instancia con report_v2 de la empresa con cpnId {cpn_id}")
            print(resp["data"])

        except ValueError as error:
            print(error)

    def get_instances_with_anulled_devolutions(self):
        try:
            print("Ingrese los cpnId separados por coma:")
            cpn_ids = input()
            cpn_id_list = [int(num) for num in cpn_ids.split(',')]

            resp = self._get_credentials(
                target={"cpn_ids": cpn_id_list}, skip=["atk_itk"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue
                resp = instance_data_service.get_instance_with_has_annulment_in_the_last_5_days()
                if not resp["success"]:
                        failed_conection_instances.append(
                            {**instance_credentials, "error": resp["error"]})
                        continue

                data_to_write.append(
                        {**instance_credentials, "devolucion_anulada":resp["data"]})

            data_csv = f"./tmp/{self.env}-devolucion_anulada.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-devolucion_anulada-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
            f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")
        except ValueError as error:
            print(error)
            
    def get_instances_with_report_v2_from_many_cpn_id(self, cpn_id_list=[]):
        try:
            if not cpn_id_list:
                print("Ingrese los cpnId separados por coma:")
                cpn_ids = input()
                cpn_id_list = [int(num) for num in cpn_ids.split(',')]
            resp = self._get_credentials(
            target={"cpn_ids": cpn_id_list}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])
            
            data_to_write = []
            failed_conection_instances = []

            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.get_instance_with_report_v2()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {"cpn_id": instance_credentials["cpn_id"], "has_report_v2":resp["data"]})

            data_csv = f"./tmp/{self.env}-report_v2.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-report_v2-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)

    def get_all_instances_with_consolidation(self):
        try:
            resp = self._get_credentials(
                target={}, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.get_instance_with_consolidation()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {**instance_credentials, **resp["data"]})

            data_csv = f"./tmp/{self.env}-get_all_instances_with_consolidation.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-get_all_instances_with_consolidation-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)

    def get_random_list_of_companies(self):
        """Obtiene lista randon de empresas"""
        try:
            print("Ingresa el total de empresas que quieres obtener:")
            quantity = input()

            print("Ingrese los cpnId separados por coma que quiera excluir:")
            cpn_ids = input()
            if cpn_ids: 
                cpn_id_list = [int(num) for num in cpn_ids.split(',')]
            else:
                cpn_id_list = []
          
            quantity = int(quantity)
            resp = self._get_credentials(target={"quantity":quantity},cpn_id_list=cpn_id_list)
            if not resp["success"]:
                raise ValueError(resp["error"])
            
            filename = f"./tmp/{self.env}-random-list.csv"
            self._write_csv(resp["data"], filename)
            print(f"Guardado terminado. Revise {filename}")
        except ValueError as error:
            print(error)   
    
    def get_companies_by_bs_menu_link(self,answer = None, ml_name = None, ml_url = None, ):
        try:

            target = {}
            if not answer:
                print("¿Desea filtrar por cpnId? (Si/No)")
                answer = input()
                if answer.lower() == "si":
                    print("Ingrese los cpnId separados por coma:")
                    cpn_ids = input()
                    cpn_id_list = [int(num) for num in cpn_ids.split(',')]
                    target = {"cpn_ids": cpn_id_list}
            if not ml_name:
                ml_name = input("Ingrese el nombre del link, Ej: 'reports_v2.sales_details': ")
            if not ml_url:
                ml_url = input("Ingrese de forma parcial o completa URL a buscar, Ej: 'gamma': ")

            resp = self._get_credentials(
                target, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])
            
            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.get_companies_by_bs_menu_link(ml_name,ml_url)

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {**instance_credentials, "success": resp["data"]})

            data_csv = f"./tmp/{self.env}-instances_for_menu_link_{ml_name}_in_environment_{ml_url}.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-instances_for_menu_link_{ml_name}_in_environment_{ml_url}-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)

    def check_active_sales_report_links(self):
        try:

            target = {}
            print("¿Desea filtrar por cpnId? (Si/No)")
            answer = input()
            if answer.lower() == "si":
                print("Ingrese los cpnId separados por coma:")
                cpn_ids = input()
                cpn_id_list = [int(num) for num in cpn_ids.split(',')]
                target = {"cpn_ids": cpn_id_list}

            resp = self._get_credentials(
                target, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])
            
            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.check_active_sales_report_links()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {**instance_credentials, "success": resp["data"]})

            data_csv = f"./tmp/{self.env}-instances_that_have_the_new_and_old_reports_active.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-instances_that_have_the_new_and_old_reports_active-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)

    def has_sales_detail_activated(self, answer=None):
        try:
            target = {}
            if not answer:
                print("¿Desea filtrar por cpnId? (Si/No)")
                answer = input()
                if answer.lower() == "si":
                    print("Ingrese los cpnId separados por coma:")
                    cpn_ids = input()
                    cpn_id_list = [int(num) for num in cpn_ids.split(',')]
                    target = {"cpn_ids": cpn_id_list}

            resp = self._get_credentials(
                target, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])
            
            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                print(instance_credentials)
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                resp = instance_data_service.has_sales_detail_activated()

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                data_to_write.append(
                    {**instance_credentials, "success": resp["data"]})

            data_csv = f"./tmp/{self.env}-instances_has_sales_detail_activated.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-instances_has_sales_detail_activated-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            for instance in data_to_write:
                print(f"cpnId: {instance['cpn_id']}, cpn_name: {instance['cpn_name']}, {instance['success']}")
            for instance in failed_conection_instances:
                print(f"cpnId: {instance['cpn_id']},cpn_name: {instance['cpn_name']}, Error: {instance['error']}")

            print(
                f"Guardado terminado. Revisa los resultados en{data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(error)


    def check_columns_exist(self, answer=None, table=None, columns=None):
        try:
            target = {}
            if not answer:
                print("¿Desea filtrar por cpnId? (Si/No)")
                answer = input()
                if answer.lower() == "si":
                    print("Ingrese los cpnId separados por coma:")
                    cpn_ids = input()
                    cpn_id_list = [int(num) for num in cpn_ids.split(',')]
                    target = {"cpn_ids": cpn_id_list}

            # Solicitar la tabla y las columnas como entradas del usuario
            if not table:
                print("Ingrese el nombre de la tabla a verificar:")
                table = input()
            if not columns:
                print(f"Ingrese las columnas de la tabla {table} a verificar (separadas por coma):")
                columns_input = input()
                columns = [col.strip() for col in columns_input.split(',')]

            resp = self._get_credentials(
                target, skip=["atk_itk", "acs_token"])
            if not resp["success"]:
                raise ValueError(resp["error"])

            data_to_write = []
            failed_conection_instances = []
            for instance_credentials in resp["data"]:
                try:
                    instance_data_service = InstanceDataService(
                        self.settings, instance_credentials)
                except Exception as error:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": str(error)})
                    continue

                # Llamamos al servicio para verificar si las columnas existen
                resp = instance_data_service.check_columns_exist(table=table, columns=columns)

                if not resp["success"]:
                    failed_conection_instances.append(
                        {**instance_credentials, "error": resp["error"]})
                    continue

                # Agregamos la respuesta junto con las credenciales
                data_to_write.append(
                    {**instance_credentials, "columns_found": resp["columns_found"], "columns_missing": resp["columns_missing"]})

            # Guardar los resultados en archivos CSV
            data_csv = f"./tmp/{self.env}-instances_check_columns_exist.csv"
            self._write_csv(data_to_write, data_csv)

            errors_csv = f"./tmp/{self.env}-instances_check_columns_exist-errors.csv"
            self._write_csv(failed_conection_instances, errors_csv)

            # Imprimir resultados en consola
            # for instance in data_to_write:
            #     print(f"cpnId: {instance['cpn_id']}, cpn_name: {instance['cpn_name']}, Columnas encontradas: {instance['columns_found']}, Columnas faltantes: {instance['columns_missing']}")
            # for instance in failed_conection_instances:
            #     print(f"cpnId: {instance['cpn_id']}, cpn_name: {instance['cpn_name']}, Error: {instance['error']}")

            df_success = pd.DataFrame(data_to_write)
            df_failed = pd.DataFrame(failed_conection_instances)

            # Imprimir los DataFrames
            print("\nResultados exitosos:")
            print(df_success.to_string(index=False))

            print("\nResultados fallidos:")
            print(df_failed.to_string(index=False))

            print(
                f"Guardado terminado. Revisa los resultados en {data_csv} y los errores en {errors_csv}")

        except ValueError as error:
            print(f"Error: {str(error)}")

    def process(self):
        options = [
            {
                "func": self.get_currency_by_cpn_id,
                "label": "Obtener moneda para una empresa"
            },
            {
                "func": self.get_currency_for_all_instances,
                "label": "Obtener moneda para todas las empresas"
            },
            {
                "func": self.get_office_summary_by_cpn_id,
                "label": "Obtener resumen de sucursales para una empresa"
            },
            {
                "func": self.get_offices_summary_for_all_instances,
                "label": "Obtener resumen de sucursales para todas las empresas"
            },
            {
                "func": self.get_sellers_summary_by_cpn_id,
                "label": "Obtener resumen de vendedores para una empresa"
            },
            {
                "func": self.get_sellers_summary_for_all_instances,
                "label": "Obtener resumen de vendedores para todas las empresas"
            },
            {
                "func": self.get_products_types_summary_by_cpn_id,
                "label": "Obtener resumen de tipos de productos para una empresa"
            },
            {
                "func": self.get_products_types_summary_for_all_instances,
                "label": "Obtener resumen de tipos de productos para todas las empresas"
            },
            {
                "func": self.get_brands_summary_by_cpn_id,
                "label": "Obtener resumen de marcas para una empresa"
            },
            {
                "func": self.get_brands_summary_for_all_instances,
                "label": "Obtener resumen de marcas para todas las empresas"
            },
            {
                "func": self.get_clients_summary_by_cpn_id,
                "label": "Obtener resumen de clientes para una empresa"
            },
            {
                "func": self.get_clients_summary_for_all_instances,
                "label": "Obtener resumen de clientes para todas las empresas"
            },
            {
                "func": self.get_instance_with_all_report_v2,
                "label": "Obtener resumen de empresas con la liberacion report_v2"
            },
            {
                "func": self.get_instances_with_report_v2_from_many_cpn_id,
                "label": "Obtener resumen de empresas con la liberacion report_v2 a partir de lista de cpn_ids"
            },
            {
                "func": self.get_instance_with_report_v2_cpn_id,
                "label": "Obtener resumen de una empresa con la liberacion report_v2"
            },
            {
                "func": self.get_instances_with_anulled_devolutions,
                "label": "Obtener resumen de empresas que realizaron anulacion de devolucion en los ultimos 5 dias"
            },
            {
                "func": self.get_all_instances_with_consolidation,
                "label": "Obtener resumen de todas las empresas con conslidacion"
            },
            {
                "func": self.get_random_list_of_companies,
                "label": "Obtener lista de empresas de forma random"
            },
            {
                "func": self.get_companies_by_bs_menu_link,
                "label": "Obtener lista de empresas por bs menu link"
            },
            {
                "func": self.check_active_sales_report_links,
                "label": "Comprobar si las empresas tienen activo el reporte de ventas viejo y detalle de ventas"
            },
            {
                "func": self.has_sales_detail_activated,
                "label": "Comprobar si las empresas tienen activo el detalle de ventas"
            },
            {
                "func": self.has_sales_detail_activated,
                "label": "Comprobar si las empresas tienen activo el detalle de ventas"
            },
            {
                "func": self.check_columns_exist,
                "label": "Verificar si las columnas existen en la tabla"
            }

        ]
        self._manage_menu_options(options=options)
