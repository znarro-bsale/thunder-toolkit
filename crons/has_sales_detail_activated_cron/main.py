from handlers.get_instance_data_handler import GetInstanceDataHandler


class HasSalesDetailActivatedCron:
    def __init__(self):
        pass

    def run(self):
        get_instance_data_handler = GetInstanceDataHandler()
        get_instance_data_handler.has_sales_detail_activated("No")