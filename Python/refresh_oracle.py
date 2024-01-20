import cx_Oracle
import pandas as pd

connstr='ACC_RISK_THIEN/Orcl2018@10.91.9.60:1521/risk'
conn = cx_Oracle.connect(connstr)

c=conn.cursor()
c.execute ('SELECT * FROM UND_BASE_LIST')


col_names = [row[0] for row in c.description]
table_col=pd.DataFrame(col_names).transpose()


for row in c:
    row=pd.DataFrame(row).transpose()
    table_col=pd.concat([table_col,row],axis=0)
