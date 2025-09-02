from crons.consolidate_report_data_cron.main import ConsolidateReportDataCron
from crons.consolidate_report_data_by_date_cron.main import ConsolidateReportDataByDateCron
from crons.configure_sales_overview_cron.main import  ConfigureSalesOverviewCron
from crons.configure_sales_details_cron.main import ConfigureSalesDetailsCron
from crons.get_companies_by_bs_menu_link_cron.main import GetCompaniesByBsMenuLinkCron
from crons.has_sales_detail_activated_cron.main import HasSalesDetailActivatedCron
from crons.check_columns_exist_cron.main import CheckColumnsExitCron
from crons.configure_dashboard_cron.main import ConfigureDashboardCron

import os
cron = os.environ.get('cron', 'configure_dashboard_cron')

crons = {
    'consolidate_report_data_cron': ConsolidateReportDataCron(),
    'consolidate_report_data_by_date_cron': ConsolidateReportDataByDateCron(),
    'configure_sales_overview_cron':ConfigureSalesOverviewCron(),
    'configure_sales_details_cron':ConfigureSalesDetailsCron(),
    'get_companies_by_bs_menu_link_cron': GetCompaniesByBsMenuLinkCron(),
    'has_sales_detail_activated_cron': HasSalesDetailActivatedCron(),
    'check_columns_exist_cron': CheckColumnsExitCron(),
    'configure_dashboard_cron': ConfigureDashboardCron()
}

crons[cron].run()
