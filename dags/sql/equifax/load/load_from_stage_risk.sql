copy into {{ params.table_name }}
    from (select
    $1:"CUST REF NBR"::varchar(50),
    $1:"NAME"::varchar(90),
    $1:"ADDRESS"::varchar(150),
    $1:"CITY"::varchar(50),
    $1:"PROVINCE CODE"::varchar(50),
    $1:"POSTAL CODE"::varchar(50),
    $1:"TELEPHONE"::varchar(50),
    $1:"FAX"::varchar(50),
    $1:"Match Ref Nbr"::varchar(50),
    $1:"SEQUENCE NUMBER"::varchar(9),
    $1:"Match Hit Flag"::varchar(1),
    $1:"Match Subject Nbr"::varchar(6),
    $1:"Match Company Nbr"::number(38,0),
    $1:"Match Company Name"::varchar(90),
    $1:"Match Company Address"::varchar(100),
    $1:"Match Company City"::varchar(30),
    $1:"Match Company Province"::varchar(2),
    $1:"Match Company Postal Code"::varchar(12),
    $1:"Match Company Phone"::varchar(10),
    $1:"Primary Subject Nbr"::varchar(6),
    $1:"Primary Company Nbr"::number(38,0),
    $1:"Prim Company Name"::varchar(90),
    $1:"Prim Company Address"::varchar(100),
    $1:"Prim Company City"::varchar(30),
    $1:"Prim Company Province"::varchar(2),
    $1:"Prim Company Postal Code"::varchar(12),
    $1:"Prim Company Phone"::varchar(10),
    try_to_date($1:"Match Company Open Date"::varchar),
    $1:"Match Comp Year Estab"::varchar(4),
    $1:"Match Company SIC Code"::varchar(8),
    $1:"Match Comp SIC Desc English"::varchar(30),
    $1:"Match Comp SIC Desc French"::varchar(30),
    $1:"Match Comp NAICS Code"::varchar(8),
    $1:"Match Comp NAICS Desc English"::varchar(30),
    $1:"Match Comp NAICS Desc French"::varchar(50),
    try_to_date($1:"Prim Company Open Date"::varchar),
    $1:"Prim Comp Year Estab"::varchar(4),
    $1:"Prim Company SIC Code"::varchar(8),
    $1:"Prim SIC Desc English"::varchar(30),
    $1:"Prim SIC Desc French"::varchar(30),
    $1:"Prim NAICS Code"::varchar(8),
    $1:"Prim NAICS Desc English"::varchar(30),
    $1:"Prim NAICS Desc French"::varchar(30),
    $1:"CI SCORE"::varchar(2),
    $1:"PI SCORE"::varchar(3),
    $1:"CDS Model Number"::varchar(5),
    $1:"CDS Reject Indicat."::varchar(1),
    $1:"CDS Reject Code"::varchar(2),
    $1:"CDS Score Value"::varchar(5),
    $1:"CDS Reason Code 1"::varchar(5),
    $1:"CDS Reason Code 2"::varchar(5),
    $1:"CDS Reason Code 3"::varchar(5),
    $1:"CDS Reason Code 4"::varchar(5),
    $1:"BFRS Model Number"::varchar(5),
    $1:"BFRS Reject Indicat."::varchar(1),
    $1:"BFRS Reject Code"::varchar(2),
    $1:"BFRS Score Value"::varchar(5),
    $1:"BFRS Reason Code 1"::varchar(5),
    $1:"BFRS Reason Code 2"::varchar(5),
    $1:"BFRS Reason Code 3"::varchar(5),
    $1:"BFRS Reason Code 4"::varchar(5),
    $1:"CS Model Number"::varchar(5),
    $1:"CS Reject Indicat."::varchar(1),
    $1:"CS Reject Code"::varchar(2),
    $1:"CS Score Value"::varchar(5),
    $1:"CS Reason Code 1"::varchar(5),
    $1:"CS Reason Code 2"::varchar(5),
    $1:"CS Reason Code 3"::varchar(5),
    $1:"CS Reason Code 4"::varchar(5),
    $1:"NBR OF COLL."::number(18,0),
    $1:"AMT OF COLL."::number(18,0),
    try_to_date($1:"LAST COLL DATE"::varchar),
    $1:"NBR OF LGL SUITS"::number(18,0),
    $1:"AMT OF LGL SUITS"::number(18,0),
    try_to_date($1:"LAST SUIT DATE"::varchar),
    $1:"NBR OF JUDGMENTS"::number(18,0),
    $1:"AMT OF JUDGMENTS"::number(18,0),
    try_to_date($1:"LAST JUDGMENT DATE"::varchar),
    $1:"NBR OF RET. CHQS"::number(18,0),
    $1:"AMT OF RET. CHQS"::number(18,0),
    try_to_date($1:"LAST RET. CHQ DATE"::varchar),
    $1:"NBR OF OTHER ITEMS"::number(18,0),
    $1:"AMT OF OTHER ITEMS"::number(18,0),
    try_to_date($1:"LAST OTHER ITEM DATE"::varchar),
    $1:"90 DAY LINES"::number(18,0),
    $1:"90 DAY TOTAL BAL"::number(18,0),
    $1:"90 DAY HIGH CREDIT"::number(18,0),
    $1:"90 DAY CURRENT"::number(18,0),
    $1:"90 DAY PD1 ODUE"::number(18,0),
    $1:"90 DAY PD2 ODUE"::number(18,0),
    $1:"90 DAY PD3 ODUE"::number(18,0),
    $1:"90 DAY CREDIT LIMIT"::number(18,0),
    $1:"90 DAY HIGHEST CREDIT"::number(18,0),
    $1:"ALL PI SCORE"::varchar(3),
    $1:"ALL LINES"::number(18,0),
    $1:"ALL TOTAL BAL"::number(18,0),
    $1:"ALL HIGH CREDIT"::number(18,0),
    $1:"ALL CURRENT"::number(18,0),
    $1:"ALL PD1 ODUE"::number(18,0),
    $1:"ALL PD2 ODUE"::number(18,0),
    $1:"ALL PD3 ODUE"::number(18,0),
    $1:"ALL CREDIT LIMIT"::number(18,0),
    $1:"ALL HIGHEST CREDIT"::number(18,0),
    $1:"DATE OF OLDEST LINE"::varchar(10),
    $1:"DATE OF NEWEST LINE"::varchar(10),
    $1:"TOTAL# OF COLL."::varchar(5),
    $1:"AMT PAID ON COLL."::number(18,0),
    $1:"# OF COLL-LAST MTH"::number(38,0),
    $1:"# OF COLL-LAST 2 MTH"::number(38,0),
    $1:"# OF COLL-LAST 3 MTH"::number(38,0),
    $1:"# OF COLL > 3 MTH"::number(38,0),
    $1:"TREND1 YEAR"::number(38,0),
    $1:"TREND1 QTR"::number(38,0),
    $1:"TREND1 # OF LINES"::number(38,0),
    $1:"TREND1 TOTAL OWING"::number(38,0),
    $1:"TREND1 CURRENT DUE"::number(38,0),
    $1:"TREND1 P1 OVERDUE"::number(38,0),
    $1:"TREND1 P2 OVERDUE"::number(38,0),
    $1:"TREND1 P3 OVERDUE"::number(38,0),
    $1:"TREND2 YEAR"::number(38,0),
    $1:"TREND2 QTR"::number(38,0),
    $1:"TREND2 # OF LINES"::number(38,0),
    $1:"TREND2 TOTAL OWING"::number(38,0),
    $1:"TREND2 CURRENT DUE"::number(38,0),
    $1:"TREND2 P1 OVERDUE"::number(38,0),
    $1:"TREND2 P2 OVERDUE"::number(38,0),
    $1:"TREND2 P3 OVERDUE"::number(38,0),
    $1:"TREND3 YEAR"::number(38,0),
    $1:"TREND3 QTR"::number(38,0),
    $1:"TREND3 # OF LINES"::number(38,0),
    $1:"TREND3 TOTAL OWING"::number(38,0),
    $1:"TREND3 CURRENT DUE"::number(38,0),
    $1:"TREND3 P1 OVERDUE"::number(38,0),
    $1:"TREND3 P2 OVERDUE"::number(38,0),
    $1:"TREND3 P3 OVERDUE"::number(38,0),
    $1:"TREND4 YEAR"::number(38,0),
    $1:"TREND4 QTR"::number(38,0),
    $1:"TREND4 # OF LINES"::number(38,0),
    $1:"TREND4 TOTAL OWING"::number(38,0),
    $1:"TREND4 CURRENT DUE"::number(38,0),
    $1:"TREND4 P1 OVERDUE"::number(38,0),
    $1:"TREND4 P2 OVERDUE"::number(38,0),
    $1:"TREND4 P3 OVERDUE"::number(38,0),
    $1:"TREND5 YEAR"::number(38,0),
    $1:"TREND5 QTR"::number(38,0),
    $1:"TREND5 # OF LINES"::number(38,0),
    $1:"TREND5 TOTAL OWING"::number(38,0),
    $1:"TREND5 CURRENT DUE"::number(38,0),
    $1:"TREND5 P1 OVERDUE"::number(38,0),
    $1:"TREND5 P2 OVERDUE"::number(38,0),
    $1:"TREND5 P3 OVERDUE"::number(38,0),
    $1:"TREND6 YEAR"::number(38,0),
    $1:"TREND6 QTR"::number(38,0),
    $1:"TREND6 # OF LINES"::number(38,0),
    $1:"TREND6 TOTAL OWING"::number(38,0),
    $1:"TREND6 CURRENT DUE"::number(38,0),
    $1:"TREND6 P1 OVERDUE"::number(38,0),
    $1:"TREND6 P2 OVERDUE"::number(38,0),
    $1:"TREND6 P3 OVERDUE"::number(38,0),
    $1:"TREND7 YEAR"::number(38,0),
    $1:"TREND7 QTR"::number(38,0),
    $1:"TREND7 # OF LINES"::number(38,0),
    $1:"TREND7 TOTAL OWING"::number(38,0),
    $1:"TREND7 CURRENT DUE"::number(38,0),
    $1:"TREND7 P1 OVERDUE"::number(38,0),
    $1:"TREND7 P2 OVERDUE"::number(38,0),
    $1:"TREND7 P3 OVERDUE"::number(38,0),
    $1:"TREND8 YEAR"::number(38,0),
    $1:"TREND8 QTR"::number(38,0),
    $1:"TREND8 # OF LINES"::number(38,0),
    $1:"TREND8 TOTAL OWING"::number(38,0),
    $1:"TREND8 CURRENT DUE"::number(38,0),
    $1:"TREND8 P1 OVERDUE"::number(38,0),
    $1:"TREND8 P2 OVERDUE"::number(38,0),
    $1:"TREND8 P3 OVERDUE"::number(38,0),
    $1:"TREND9 YEAR"::number(38,0),
    $1:"TREND9 QTR"::number(38,0),
    $1:"TREND9 # OF LINES"::number(38,0),
    $1:"TREND9 TOTAL OWING"::number(38,0),
    $1:"TREND9 CURRENT DUE"::number(38,0),
    $1:"TREND9 P1 OVERDUE"::number(38,0),
    $1:"TREND9 P2 OVERDUE"::number(38,0),
    $1:"TREND9 P3 OVERDUE"::number(38,0),
    $1:"INQUIRY CNT"::number(38,0),
    try_to_date($1:"I001 DATE"::varchar),
    $1:"I001 SIC"::varchar(4),
    $1:"I001 MBR NAME"::varchar(30),
    try_to_date($1:"I002 DATE"::varchar),
    $1:"I002 SIC"::varchar(4),
    $1:"I002 MBR NAME"::varchar(30),
    try_to_date($1:"I003 DATE"::varchar),
    $1:"I003 SIC"::varchar(4),
    $1:"I003 MBR NAME"::varchar(30),
    try_to_date($1:"I004 DATE"::varchar),
    $1:"I004 SIC"::varchar(4),
    $1:"I004 MBR NAME"::varchar(30),
    try_to_date($1:"I005 DATE"::varchar),
    $1:"I005 SIC"::varchar(4),
    $1:"I005 MBR NAME"::varchar(30),
    try_to_date($1:"I006 DATE"::varchar),
    $1:"I006 SIC"::varchar(4),
    $1:"I006 MBR NAME"::varchar(30),
    try_to_date($1:"I007 DATE"::varchar),
    $1:"I007 SIC"::varchar(4),
    $1:"I007 MBR NAME"::varchar(30),
    try_to_date($1:"I008 DATE"::varchar),
    $1:"I008 SIC"::varchar(4),
    $1:"I008 MBR NAME"::varchar(30),
    try_to_date($1:"I009 DATE"::varchar),
    $1:"I009 SIC"::varchar(4),
    $1:"I009 MBR NAME"::varchar(30),
    try_to_date($1:"I010 DATE"::varchar),
    $1:"I010 SIC"::varchar(4),
    $1:"I010 MBR NAME"::varchar(30),
    try_to_date($1:"I011 DATE"::varchar),
    $1:"I011 SIC"::varchar(4),
    $1:"I011 MBR NAME"::varchar(30),
    try_to_date($1:"I012 DATE"::varchar),
    $1:"I012 SIC"::varchar(4),
    $1:"I012 MBR NAME"::varchar(30),
    try_to_date($1:"I013 DATE"::varchar),
    $1:"I013 SIC"::varchar(4),
    $1:"I013 MBR NAME"::varchar(30),
    try_to_date($1:"I014 DATE"::varchar),
    $1:"I014 SIC"::varchar(4),
    $1:"I014 MBR NAME"::varchar(30),
    try_to_date($1:"I015 DATE"::varchar),
    $1:"I015 SIC"::varchar(4),
    $1:"I015 MBR NAME"::varchar(30),
    try_to_date($1:"I016 DATE"::varchar),
    $1:"I016 SIC"::varchar(4),
    $1:"I016 MBR NAME"::varchar(30),
    try_to_date($1:"I017 DATE"::varchar),
    $1:"I017 SIC"::varchar(4),
    $1:"I017 MBR NAME"::varchar(30),
    try_to_date($1:"I018 DATE"::varchar),
    $1:"I018 SIC"::varchar(4),
    $1:"I018 MBR NAME"::varchar(30),
    try_to_date($1:"I019 DATE"::varchar),
    $1:"I019 SIC"::varchar(4),
    $1:"I019 MBR NAME"::varchar(30),
    try_to_date($1:"I020 DATE"::varchar),
    $1:"I020 SIC"::varchar(4),
    $1:"I020 MBR NAME"::varchar(30),
    try_to_date($1:"I021 DATE"::varchar),
    $1:"I021 SIC"::varchar(4),
    $1:"I021 MBR NAME"::varchar(30),
    try_to_date($1:"I022 DATE"::varchar),
    $1:"I022 SIC"::varchar(4),
    $1:"I022 MBR NAME"::varchar(30),
    try_to_date($1:"I023 DATE"::varchar),
    $1:"I023 SIC"::varchar(4),
    $1:"I023 MBR NAME"::varchar(30),
    try_to_date($1:"I024 DATE"::varchar),
    $1:"I024 SIC"::varchar(4),
    $1:"I024 MBR NAME"::varchar(30),
    try_to_date($1:"I025 DATE"::varchar),
    $1:"I025 SIC"::varchar(4),
    $1:"I025 MBR NAME"::varchar(30),
    try_to_date($1:"I026 DATE"::varchar),
    $1:"I026 SIC"::varchar(4),
    $1:"I026 MBR NAME"::varchar(30),
    try_to_date($1:"I027 DATE"::varchar),
    $1:"I027 SIC"::varchar(4),
    $1:"I027 MBR NAME"::varchar(30),
    try_to_date($1:"I028 DATE"::varchar),
    $1:"I028 SIC"::varchar(4),
    $1:"I028 MBR NAME"::varchar(30),
    try_to_date($1:"I029 DATE"::varchar),
    $1:"I029 SIC"::varchar(4),
    $1:"I029 MBR NAME"::varchar(30),
    try_to_date($1:"I030 DATE"::varchar),
    $1:"I030 SIC"::varchar(4),
    $1:"I030 MBR NAME"::varchar(30),
    try_to_date($1:"I031 DATE"::varchar),
    $1:"I031 SIC"::varchar(4),
    $1:"I031 MBR NAME"::varchar(30),
    try_to_date($1:"I032 DATE"::varchar),
    $1:"I032 SIC"::varchar(4),
    $1:"I032 MBR NAME"::varchar(30),
    try_to_date($1:"I033 DATE"::varchar),
    $1:"I033 SIC"::varchar(4),
    $1:"I033 MBR NAME"::varchar(30),
    try_to_date($1:"I034 DATE"::varchar),
    $1:"I034 SIC"::varchar(4),
    $1:"I034 MBR NAME"::varchar(30),
    try_to_date($1:"I035 DATE"::varchar),
    $1:"I035 SIC"::varchar(4),
    $1:"I035 MBR NAME"::varchar(30),
    try_to_date($1:"I036 DATE"::varchar),
    $1:"I036 SIC"::varchar(4),
    $1:"I036 MBR NAME"::varchar(30),
    try_to_date($1:"I037 DATE"::varchar),
    $1:"I037 SIC"::varchar(4),
    $1:"I037 MBR NAME"::varchar(30),
    try_to_date($1:"I038 DATE"::varchar),
    $1:"I038 SIC"::varchar(4),
    $1:"I038 MBR NAME"::varchar(30),
    try_to_date($1:"I039 DATE"::varchar),
    $1:"I039 SIC"::varchar(4),
    $1:"I039 MBR NAME"::varchar(30),
    try_to_date($1:"I040 DATE"::varchar),
    $1:"I040 SIC"::varchar(4),
    $1:"I040 MBR NAME"::varchar(30),
    try_to_date($1:"I041 DATE"::varchar),
    $1:"I041 SIC"::varchar(4),
    $1:"I041 MBR NAME"::varchar(30),
    try_to_date($1:"I042 DATE"::varchar),
    $1:"I042 SIC"::varchar(4),
    $1:"I042 MBR NAME"::varchar(30),
    try_to_date($1:"I043 DATE"::varchar),
    $1:"I043 SIC"::varchar(4),
    $1:"I043 MBR NAME"::varchar(30),
    try_to_date($1:"I044 DATE"::varchar),
    $1:"I044 SIC"::varchar(4),
    $1:"I044 MBR NAME"::varchar(30),
    try_to_date($1:"I045 DATE"::varchar),
    $1:"I045 SIC"::varchar(4),
    $1:"I045 MBR NAME"::varchar(30),
    try_to_date($1:"I046 DATE"::varchar),
    $1:"I046 SIC"::varchar(4),
    $1:"I046 MBR NAME"::varchar(30),
    try_to_date($1:"I047 DATE"::varchar),
    $1:"I047 SIC"::varchar(4),
    $1:"I047 MBR NAME"::varchar(30),
    try_to_date($1:"I048 DATE"::varchar),
    $1:"I048 SIC"::varchar(4),
    $1:"I048 MBR NAME"::varchar(30),
    try_to_date($1:"I049 DATE"::varchar),
    $1:"I049 SIC"::varchar(4),
    $1:"I049 MBR NAME"::varchar(30),
    try_to_date($1:"I050 DATE"::varchar),
    $1:"I050 SIC"::varchar(4),
    $1:"I050 MBR NAME"::varchar(30),
    $1:imported_file_name::varchar(250),
    $1:import_month::varchar(6),
    try_to_timestamp($1:import_ts::varchar(30))
    from @{{ params.stage_name }});