import math
import random
import pandas as pd
from typing import List, Dict, Any
from models.metadatabase_model import MetadatabaseModel
from helpers.database_manager import DatabaseManager
import subprocess

from models.redis_model import RedisManager


class InstanceCredentialsService(DatabaseManager):
    def __init__(self, settings: Dict[str, Any]):
        connection = self._get_connection(settings["bway"])
        self.metadatabase_model = MetadatabaseModel(connection)
        self.redis_manager = RedisManager()
        self.redis_client = self.redis_manager._get_connection(settings["redis"])

    def _merge_lists(self, *lists):
        if not any(lists):
            return []

        merged_list = []

        # Create a dictionary for each list based on 'cpn_id'
        cpn_id_dicts = [{item['cpn_id']: item for item in lst}
                        for lst in lists]

        # Combine all 'cpn_id' values from all dictionaries
        all_cpn_ids = set().union(*cpn_id_dicts)

        for cpn_id in all_cpn_ids:
            merged_item = {'cpn_id': cpn_id}

            # Merge items with the same 'cpn_id'
            for cpn_id_dict in cpn_id_dicts:
                item = cpn_id_dict.get(cpn_id, {})
                merged_item.update(item)

            merged_list.append(merged_item)

        return merged_list

    def _encrypt(self, data):
        """Encripta un texto con AES-128-CBC"""
        ruby_script = './subprocess/encryptor.rb'
        try:
            # Instala las gemas necesarias
            script_arguments = [data]
            result = subprocess.run(
                ["ruby", ruby_script] + script_arguments, check=True, text=True, capture_output=True)
            output = result.stdout
            resp = output.strip()
            return resp

        except Exception as e:
            return str(e)

    def _get_atk_itk_for_cpn_id_list(self, cpn_id_list: List[int]):
        """Busca los ids de los atk e itk y los encripta"""
        resp = {}
        try:
            metadb_resp = self.metadatabase_model.get_instance_atk_itk_by_cpn_id(
                cpn_id_list=cpn_id_list)
            if not metadb_resp["success"]:
                raise ValueError(metadb_resp["error"])

            resp["data"] = []
            for r in metadb_resp["data"]:
                atk_data = r["atk_id"]
                itk_data = r["itk_id"]
                resp["data"].append({"cpn_id": r["cpn_id"], "atk": self._encrypt(str(atk_data)),
                                     "itk": self._encrypt(str(itk_data))})

            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    def _get_all_cpn_atk_itk(self):
        """Obtiene todas las empresas de desarrollo"""
        resp = {}
        try:
            metadb_resp = self.metadatabase_model.get_all_instances_atk_itk()
            if not metadb_resp["success"]:
                raise ValueError(resp["error"])

            print(
                f"Encriptando atk e itk para {len(metadb_resp['data'])} instancias en total")
            resp["data"] = []
            for r in metadb_resp["data"]:
                atk_data = r["atk_id"]
                itk_data = r["itk_id"]
                resp["data"].append({"cpn_id": r["cpn_id"], "atk": self._encrypt(str(atk_data)),
                                     "itk": self._encrypt(str(itk_data))})

            resp["success"] = True
        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
        return resp

    def get_credentials_for_cpn_id_list(self, cpn_id_list: List[int], skip: List[str] = []):
        resp = {}
        try:
            lists_to_merge = []
            if not 'db_access' in skip:
                db_access_data_resp = self.metadatabase_model.get_instance_db_access_data_by_cpn_id(
                    cpn_id_list)
                if not db_access_data_resp["success"]:
                    raise ValueError(db_access_data_resp["error"])

                lists_to_merge.append(db_access_data_resp["data"])

            atk_itk_data_resp = {}
            if not 'atk_itk' in skip:
                atk_itk_data_resp = self._get_atk_itk_for_cpn_id_list(
                    cpn_id_list=cpn_id_list)
                if not atk_itk_data_resp["success"]:
                    raise ValueError(atk_itk_data_resp["error"])

                lists_to_merge.append(atk_itk_data_resp["data"])

            if not 'acs_token' in skip:
                acs_token_data_resp = self.metadatabase_model.get_access_token_by_cpn_id(
                    cpn_id_list=cpn_id_list)
                if not acs_token_data_resp["success"]:
                    raise ValueError(acs_token_data_resp["error"])

                lists_to_merge.append(acs_token_data_resp["data"])

            resp["data"] = self._merge_lists(*lists_to_merge)
            resp["success"] = True
        except ValueError as e:
            resp["error"] = str(e)
            resp["success"] = False

        self._close_connection()
        return resp

    def get_credentials_for_all(self, skip: List[str] = []) -> Dict[str, Any]:
        resp = {}
        try:
            lists_to_merge = []
            if not 'db_access' in skip:
                db_access_data_resp = self.metadatabase_model.get_all_instances_db_access_data()
                if not db_access_data_resp["success"]:
                    raise ValueError(db_access_data_resp["error"])

                lists_to_merge.append(db_access_data_resp["data"])

            if not 'atk_itk' in skip:
                atk_itk_data_resp = self._get_all_cpn_atk_itk()
                if not atk_itk_data_resp["success"]:
                    raise ValueError(atk_itk_data_resp["error"])

                lists_to_merge.append(atk_itk_data_resp["data"])

            if not 'acs_token' in skip:
                acs_token_data_resp = self.metadatabase_model.get_all_instances_access_token()
                if not acs_token_data_resp["success"]:
                    raise ValueError(acs_token_data_resp["error"])

                lists_to_merge.append(acs_token_data_resp["data"])

            resp["data"] = self._merge_lists(*lists_to_merge)
            resp["success"] = True
        except ValueError as e:
            resp["error"] = str(e)
            resp["success"] = False

        self._close_connection()
        return resp
    
    def get_random_db_access_credentials(self, quantity, cpn_id_list):
        try:
            resp = self.metadatabase_model.get_all_db_ips()
            if not resp["success"]:
                raise ValueError(resp["error"])
            
            clusters = resp["data"]
            n_clusters = len(clusters)

            print("+" * 100)
            print(f"Numero de Cluster: {n_clusters}")
            print("+" * 100)
            
            sample = []
            ids_to_exclude = cpn_id_list
            remaining = quantity
        
            for count in range(1, 20):
                print("+" * 100)
                print(f"************Buscando {remaining} empresas****************")
                print(f"Intento N°{count}")
                print("+" * 100)
                
                if remaining <= 0:
                    break
                
                for cluster in clusters:
                    db_access_data_resp = self.metadatabase_model.get_instance_db_access_data_by_randon_db_ip(
                        db_ip=cluster["cpn_dbase_ip"],
                        exclude_ids=ids_to_exclude,
                    )
                    if not db_access_data_resp["success"]:
                        raise ValueError(db_access_data_resp["error"])
                    
                    cpns_per_cluster = math.ceil(remaining / n_clusters)
                    sample_size = min(cpns_per_cluster, len(db_access_data_resp["data"]))
                    current_cluster_sample = random.sample(db_access_data_resp["data"], sample_size)

                    sample.extend(current_cluster_sample)

                sample_set = list({item["cpn_id"]: item for item in sample}.values())

                new_ids = [item["cpn_id"] for item in sample_set]
                ids_to_exclude = new_ids + cpn_id_list
                remaining = quantity - len(sample_set)

            if len(sample_set) > quantity:
                sample_set = random.sample(sample_set, quantity)

            if len(sample_set) < quantity:
                print("&" * 100)
                print("No se obtuvo la totalidad de las empresas")
                print("Total empresas obtenidas:", len(sample_set))
                remaining = quantity - len(sample_set)
                print("Total empresas faltantes:", remaining)
                print("&" * 100)

            unique_ids = [item["cpn_id"] for item in sample_set]

            redis_key = "foundation_sales_reports_release_list_ids"
            self.redis_manager.set_value(
                redis_key,
                ",".join(map(str, unique_ids)),
                ex=3 * 24 * 60 * 60,
            )

            df = pd.DataFrame(sample_set)
            db_counts = df['cpn_dbase_ip'].value_counts()
            country_counts = df['cpn_country'].value_counts()

            print("*" * 90)
            print("Total empresas obtenidas:")
            print(len(unique_ids))
            print("*" * 90)
            print(db_counts)
            print(country_counts)

            return {"success": True, "data": sample_set}

        except ValueError as e:
            return {"success": False, "error": str(e)}

        finally:
            self._close_connection()
            self.redis_manager._close_connection()


def remove_duplicates_by_key(items, key):
    seen = set()
    unique_items = []
    duplicates = []
    for item in items:
        value = item[key]
        if value not in seen:
            unique_items.append(item)
            seen.add(value)
        else:
            duplicates.append(item)
    return unique_items
