from typing import List, Dict
from models.multidatabase_model import MultidatabaseModel


class CurrencyModel(MultidatabaseModel):
    def get_first_currency(self):
        resp = {}
        try:
            query = """
                    SELECT * FROM moneda LIMIT 1;
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
            
