from helpers.api_client import ApiClient


class DataConsolidationModel(ApiClient):
    def __init__(self, api_list, use_proxies):
        super().__init__(api_list["dataConsolidationAPI"], use_proxies)

    # def _handle_request(self, endpoint, method, headers, payload=None):
    #     resp = {}
    #     try:
    #         response = self._call(
    #             endpoint=endpoint, headers=headers, method=method, payload=payload)
    #         print("#"*100)
    #         print(response)
    #         print("#"*100)
    #         if not response["success"]:
    #             raise ValueError(response["error"])
            
    #         api_response = response["data"]
    #         if 'errors' in api_response:
    #             error_message = api_response['errors']
    #             raise ValueError(
    #                 f"Error: {error_message} - code: {api_response['code']}")

    #         resp["data"] = api_response['data']
    #         resp["success"] = True
    #     except ValueError as error:
    #         resp["error"] = str(error)
    #         resp["success"] = False
    #     return resp

    def remove_data(self, access_token):
        headers = {"access_token": access_token}

        return self._call(endpoint="/v1/sales/reports.json", headers=headers, method="DELETE")

    def consolidate_all_data(self, access_token):
        headers = {"access_token": access_token}
        payload = {}
        return self._call(endpoint="/v1/sales/reports.json", headers=headers, payload=payload, method="POST")

    def consolidate_data_by_range(self, access_token, start_document_id, end_document_id):
        headers = {"access_token": access_token}
        payload = {
            "start_document_id": start_document_id,
            "end_document_id": end_document_id
        }
        return self._call(endpoint="/v1/sales/reports.json", headers=headers, payload=payload, method="POST")
    
    def consolidate_data_by_date(self, access_token, start_date, end_date):
        headers = {"access_token": access_token}
        payload = {
            "start_date": start_date,
            "end_date": end_date
        }
        return self._call(endpoint="/v1/sales/reports.json", headers=headers, payload=payload, method="POST")
