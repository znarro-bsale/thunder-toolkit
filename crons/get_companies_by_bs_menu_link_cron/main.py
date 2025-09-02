from handlers.get_instance_data_handler import GetInstanceDataHandler


class GetCompaniesByBsMenuLinkCron:
    def __init__(self):
        pass

    def run(self):
   
        get_instance_data_handler = GetInstanceDataHandler()
        get_instance_data_handler.get_companies_by_bs_menu_link(
            "No", "reports_v2.sales_details", "gamma"
        )