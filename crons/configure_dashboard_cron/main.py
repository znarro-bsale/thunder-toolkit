from handlers.report_data_consolidation_handler import ReportDataConsolidationHandler


class ConfigureDashboardCron:
    def __init__(self):
        pass

    def run(self):
   
        report_data_consolidation_hanlder = ReportDataConsolidationHandler()
        report_data_consolidation_hanlder.set_reportv2_for_many_cpn_id_concurrently(
            [], "prod", "dashboard"
        )
