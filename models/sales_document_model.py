from models.multidatabase_model import MultidatabaseModel


class SalesDocumentModel(MultidatabaseModel):
    def get_documents_count(self):
        resp = {}
        try:
            query = """
            SELECT
            min( id_venta_documento_tributario ) as min,
            max( id_venta_documento_tributario ) as max 
            FROM
	        venta_documento_tributario;
          """
            r = self._execute_query(query, ())
            if not r["success"]:
                raise ValueError(r["error"])
    
            resp["success"] = True
            resp["data"] = r["data"][0]

        except ValueError as error:
            resp["error"] = str(error)
            resp["success"] = False
            
        return resp
