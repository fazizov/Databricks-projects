-- Databricks notebook source
INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental
with p as (
  SELECT
    *,
    if(
      isnotnull(
        if(networth > 1000000 or income > 200000,"HighValue+","") || 
        if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
        if(age > 45, "Boomer+", "") ||
        if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
        if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
        if(age < 25 and networth > 1000000, "Inherited+","")),
      left(
        if(networth > 1000000 or income > 200000,"HighValue+","") || 
        if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
        if(age > 45, "Boomer+", "") ||
        if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
        if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
        if(age < 25 and networth > 1000000, "Inherited+",""),
        length(
          if(networth > 1000000 or income > 200000,"HighValue+","") || 
          if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
          if(age > 45, "Boomer+", "") ||
          if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
          if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
          if(age < 25 and networth > 1000000, "Inherited+",""))
        -1),
      NULL) marketingnameplate,
    int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch*",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => ",",
    fileNamePattern => "Prospect.csv",
    schema => "agencyid STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, state STRING, country STRING, phone STRING, income STRING, numbercars INT, numberchildren INT, maritalstatus STRING, age INT, creditrating INT, ownorrentflag STRING, employer STRING, numbercreditcards INT, networth INT"
  )
)
SELECT * FROM (
  SELECT
    * except(batchid),
    max(batchid) recordbatchid,
    min(batchid) batchid
  FROM p
  GROUP BY ALL
)
WHERE recordbatchid = 3;
