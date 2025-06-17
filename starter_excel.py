from bitfount import Pod
from bitfount import MyExcelSource

pod = Pod(
    name="mt-excel-datasource",
    datasource=MyExcelSource(
        "/home/martinb/workspace/bitfount-intermine/test.xlsx",
        sheet_name="Sheet2",
    ),
)
pod.start()
