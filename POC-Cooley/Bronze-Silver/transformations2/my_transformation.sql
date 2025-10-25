CREATE STREAMING LIVE TABLE silver.Salesdetails;
CREATE FLOW flow_Salesdetails
AS AUTO CDC INTO LIVE.Salesdetails
FROM bronze.salesorderdetail
KEYS (SalesOrderDetailID)
APPLY AS DELETE WHEN  `__$operation`=1
SEQUENCE BY `__start_lsn`
COLUMNS * EXCEPT (`__$operation`); 

