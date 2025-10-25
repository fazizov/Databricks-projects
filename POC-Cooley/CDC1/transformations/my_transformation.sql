Create  STREAMING LIVE TABLE Salesdetails;
APPLY CHANGES INTO LIVE.Salesdetails
FROM stream(ms_sql_fc.cdc.saleslt_salesorderdetail_ct)
  KEYS (SalesOrderDetailID)
  APPLY AS DELETE WHEN `__$operation` = "DELETE"
  SEQUENCE BY `__$seqval`
  COLUMNS * EXCEPT (`__$operation`);    