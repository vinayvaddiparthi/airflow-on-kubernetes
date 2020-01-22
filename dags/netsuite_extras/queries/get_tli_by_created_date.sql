-- create table for the selected created_date,
-- get erp required fields: new/old gl, account_internal_id, subsidiary_internal_id
-- get task required fields: tran_id, doc_description(for next stage process categorizing)
create or replace table erp.{{env}}.tli_{{create_date_trim}} as
select tli.id                                                                     as id,
       tran.id                                                                    as tran_id,
       coa.new_gl                                                                 as new_gl,
       coa.old_gl                                                                 as old_gl,
       coa.account_id                                                             as account_internal_id,
       coa.subsidiary                                                             as subsidiary,
       coa.subsidiary_id                                                          as subsidiary_id,
       coalesce(tran.c2g__documentdescription__c, tran.c2g__documentreference__c) as document_description,
       tli.credit__c                                                              as credit,
       tli.debit__c                                                               as debit,
       tli.transaction_date__c                                                    as tran_date,
       tli.createddate                                                            as created_date
from salesforce.sfoi.c2g__codatransactionlineitem__c tli
         left join salesforce.sfoi.c2g__codatransaction__c tran on tran.id = tli.c2g__transaction__c
         left join erp.public.chart_of_account coa on old_gl = trim(to_char(tli.general_ledger_account_number__c))
         left join erp.test.tli_uploaded uploaded on uploaded.id = tli.id
where uploaded.id is null
  and to_date(tli.createddate) = '{{created_date}}'
  and to_date(tli.transaction_date__c) >= '2020-01-01'
  and tli.createdbyid in ('005700000057inaAAA', '0050g000005rVycAAE');
