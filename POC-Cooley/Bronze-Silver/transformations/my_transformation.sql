Create  materialized view cdc_Salesdetails
as
Select * from ms_sql_fc.cdc.saleslt_salesorderdetail_ct;

create  STREAMING LIVE TABLE Salesdetails;
APPLY CHANGES INTO LIVE.Salesdetails
FROM stream(cdc_Salesdetails)
  KEYS (SalesOrderDetailID)
  SEQUENCE BY `__$seqval`
  COLUMNS * EXCEPT (`__$operation`); 

