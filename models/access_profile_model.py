from models.multidatabase_model import MultidatabaseModel


class AccessProfileModel(MultidatabaseModel):
    def get_access_profiles(self):
        query = """
                SELECT id_perfil_acceso FROM perfil_acceso WHERE estado_perfil = 0
            """
        return self._execute_query(query, ())

    def give_access_to_profile_by_module_action(self,  profile_id: int, action_module: str):
        query = """
                INSERT INTO perfil_accion (id_perfil_acceso, id_accion_modulo)
                VALUES (%s, (SELECT id_accion_modulo FROM accion_modulo WHERE nombre_accion_i18n = %s));
            """
        return self._execute_query(query, (profile_id, action_module), False)
