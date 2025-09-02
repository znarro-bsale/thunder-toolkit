from handlers.get_instance_data_handler import GetInstanceDataHandler


class CheckColumnsExitCron:
    def __init__(self):
        pass

    def run(self):
        get_instance_data_handler = GetInstanceDataHandler()
        get_instance_data_handler.check_columns_exist(
            "No",
            "vw_sales_reports",
            ["id_descuento","id_cupon","trackingNumber","serialNumberPrefix","fullSerialNumber","uuid","stampDate"]
            )