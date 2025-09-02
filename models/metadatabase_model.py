from typing import List
from models.multidatabase_model import MultidatabaseModel


class MetadatabaseModel(MultidatabaseModel):
    def get_instance_db_access_data_by_cpn_id(self, cpn_id_list: List[int]):
        """Get instance data by company ID"""
        query = """
            SELECT tc.cpn_id, tc.cpn_name, CONCAT(ts.sys_dbaseprefix, tc.cpn_dbase) as cpn_db_name, tc.cpn_dbase_ip, tc.cpn_country 
            FROM tbw_companies tc 
            INNER JOIN tbw_systems ts ON ts.sys_id = tc.sys_id  
            WHERE tc.cpn_id IN (%s)
            AND ts.sys_id = 10 
            AND tc.cpn_state = 0 
            AND (tc.cpn_trial = 0 OR tc.cpn_trial_end > CURRENT_DATE())
            AND tc.cpn_dbase_ip NOT IN ('10.10.2.5', '10.10.2.21', '----mdb-bsale-9.c2faipgbmpkg.us-east-1.rds.amazonaws.com')
        """
        placeholders = ', '.join(['%s' for _ in cpn_id_list])
        full_query = query % placeholders

        return self._execute_query(full_query, tuple(cpn_id_list))

    def get_instance_db_access_data_by_db_ip(self, db_ip: str):
        """Get instance data by database IP"""
        query = """
            SELECT tc.cpn_id, tc.cpn_name, CONCAT(ts.sys_dbaseprefix, tc.cpn_dbase) as cpn_db_name, tc.cpn_dbase_ip 
            FROM tbw_companies tc 
            INNER JOIN tbw_systems ts ON ts.sys_id = tc.sys_id  
            WHERE tc.cpn_dbase_ip = %s 
            AND ts.sys_id = 10 
            AND tc.cpn_state = 0
            AND (tc.cpn_trial = 0 OR tc.cpn_trial_end > CURRENT_DATE())
            AND tc.cpn_dbase_ip NOT IN ('10.10.2.5', '10.10.2.21', '----mdb-bsale-9.c2faipgbmpkg.us-east-1.rds.amazonaws.com')
        """
        return self._execute_query(query, (db_ip,))

    def get_all_instances_db_access_data(self):
        query = """
            SELECT tc.cpn_id, tc.cpn_name, CONCAT(ts.sys_dbaseprefix, tc.cpn_dbase) as cpn_db_name, tc.cpn_dbase_ip 
            FROM tbw_companies tc 
            INNER JOIN tbw_systems ts ON ts.sys_id = tc.sys_id  
            WHERE ts.sys_id = 10 
            AND tc.cpn_state = 0 
            AND (tc.cpn_trial = 0 OR tc.cpn_trial_end > CURRENT_DATE())
            AND tc.cpn_dbase_ip NOT IN ('10.10.2.5', '10.10.2.21', '----mdb-bsale-9.c2faipgbmpkg.us-east-1.rds.amazonaws.com')
            AND tc.cpn_dbase_ip NOT LIKE '%%inactive%%'
            AND tc.cpn_dbase_ip NOT LIKE '%%inicial%%'
            AND tc.cpn_dbase_ip NOT LIKE '%%nasa%%'
        """
        return self._execute_query(query, ())

    def get_all_db_ips(self):
        """Get all database IPs based on specified conditions"""
        query = """
                SELECT DISTINCT tc.cpn_dbase_ip 
                FROM tbw_companies tc 
                INNER JOIN tbw_systems ts ON ts.sys_id = tc.sys_id  
                WHERE ts.sys_id = 10 
                AND tc.cpn_state = 0 
                AND (tc.cpn_trial = 0 OR tc.cpn_trial_end > CURRENT_DATE())
                AND tc.cpn_dbase_ip NOT IN ('10.10.2.5', '10.10.2.21', '----mdb-bsale-9.c2faipgbmpkg.us-east-1.rds.amazonaws.com')
                AND tc.cpn_dbase_ip NOT LIKE '%%inactive%%'
                AND tc.cpn_dbase_ip NOT LIKE '%%inicial%%'
                AND tc.cpn_dbase_ip NOT LIKE '%%nasa%%'
            """
        return self._execute_query(query)

    def get_all_instances_atk_itk(self):
        """Get all database IPs based on specified conditions"""
        query = """
                SELECT cpn_id, atk_id, itk_id 
                FROM tbw_instance_token 
                WHERE itk_json IS NOT NULL 
                GROUP BY cpn_id
            """
        return self._execute_query(query)

    def get_instance_atk_itk_by_cpn_id(self, cpn_id_list: List[int]):
        """Get instance ATK and ITK by company ID"""
        query = """
                SELECT cpn_id, atk_id, itk_id 
                FROM tbw_instance_token 
                WHERE cpn_id IN (%s) AND itk_json IS NOT NULL 
                GROUP BY cpn_id
            """
        placeholders = ', '.join(['%s' for _ in cpn_id_list])

        full_query = query % placeholders

        return self._execute_query(full_query, tuple(cpn_id_list))

    def get_access_token_by_cpn_id(self, cpn_id_list: List[int]):
        query = """
                SELECT cpn_id, acs_token 
                FROM tbw_access 
                WHERE cpn_id IN ( %s ) AND usr_id = 48 
                GROUP BY cpn_id
        """
        placeholders = ', '.join(['%s' for _ in cpn_id_list])
        full_query = query % placeholders

        return self._execute_query(full_query, tuple(cpn_id_list))

    def get_all_instances_access_token(self):
        query = """
                SELECT ta.cpn_id, ta.acs_token 
                FROM tbw_access ta
                INNER JOIN tbw_companies tc ON ta.cpn_id = tc.cpn_id
			    INNER JOIN tbw_systems ts ON ts.sys_id = tc.sys_id  
                WHERE ta.usr_id = 48
                AND ts.sys_id = 10 
				AND tc.cpn_state = 0 
				AND (tc.cpn_trial = 0 OR tc.cpn_trial_end > CURRENT_DATE())
				AND tc.cpn_dbase_ip NOT IN ('10.10.2.5', '10.10.2.21', '----mdb-bsale-9.c2faipgbmpkg.us-east-1.rds.amazonaws.com')
				AND tc.cpn_dbase_ip NOT LIKE '%%inactive%%'
				AND tc.cpn_dbase_ip NOT LIKE '%%inicial%%'
				AND tc.cpn_dbase_ip NOT LIKE '%%nasa%%'
                GROUP BY cpn_id
        """

        return self._execute_query(query, ())
    
    def get_instance_db_access_data_by_randon_db_ip(self, db_ip: str,exclude_ids: List[int]):
        """Get instance data by database IP"""
        base_query = """
            SELECT DISTINCT(tc.cpn_id), tc.cpn_name, CONCAT(ts.sys_dbaseprefix, tc.cpn_dbase) as cpn_db_name, tc.cpn_dbase_ip, tc.cpn_country
            FROM tbw_companies tc 
            INNER JOIN tbw_systems ts ON ts.sys_id = tc.sys_id  
            WHERE tc.cpn_dbase_ip = '%s' 
            AND ts.sys_id = 10 
            AND tc.cpn_state = 0 
            AND (tc.cpn_trial = 0 OR tc.cpn_trial_end > CURRENT_DATE())
            AND tc.cpn_dbase_ip NOT IN ('10.10.2.5', '10.10.2.21', '----mdb-bsale-9.c2faipgbmpkg.us-east-1.rds.amazonaws.com')
            AND tc.cpn_dbase_ip NOT LIKE '%%trial%%'
            AND tc.cpn_dbase_ip NOT LIKE '%%inactive%%'
            AND tc.cpn_dbase_ip NOT LIKE '%%inicial%%'
            AND tc.cpn_dbase_ip NOT LIKE '%%nasa%%'
        """
        # Agregar la cláusula de exclusión si la lista de IDs no está vacía
        if exclude_ids:
            ids_to_exclude_str = ', '.join(map(str, exclude_ids))
            exclusion_clause = "AND tc.cpn_id NOT IN (%s)" % ids_to_exclude_str
            query = base_query + exclusion_clause
        else:
            query = base_query
    
        # Formatear la consulta con los valores
        formatted_query = query % db_ip
        return self._execute_query(formatted_query)