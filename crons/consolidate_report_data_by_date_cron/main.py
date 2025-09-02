from handlers.report_data_consolidation_handler import ReportDataConsolidationHandler
from datetime import datetime, timedelta


class ConsolidateReportDataByDateCron():
    def __init__(self):
        pass

    def run(self):
        cpn_id_list = "all"
        start_date = (datetime.now() - timedelta(days=14)).timestamp()
        end_date = (datetime.now()).timestamp()

        report_data_consolidation_hanlder = ReportDataConsolidationHandler()
        report_data_consolidation_hanlder.set_up_consolidation_by_date(
            cpn_id_list, start_date, end_date)
