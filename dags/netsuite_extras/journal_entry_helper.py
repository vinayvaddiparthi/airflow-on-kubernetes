MP = f""
LC_TC_ARIO = f""
FL = f""
NSF = f""
LIA = f""
FMT_EQB = f""
PF_MCAs = f""
CG = f""


def getMP():
    print()


process = {
    "Manual Payment": getMP()
}


def get_manual_payments(created_date, created_by):
    process_mp = f"""
        select coa.new_account,
               tli.transaction_date__c,
               tli.c2g__linedescription__c,
               tli.c2g__linereference__c,
               je.c2g__journaldescription__c,
               sum(tli.debit__c)      as debit,
               sum(tli.credit__c)     as credit,
               sum(c2g__homevalue__c) as balance,
               coa.subsidiary as subsidiary,
               aid.internal_id
        from salesforce.sfoi.c2g__codatransactionlineitem__c as tli
                 left join salesforce.sfoi.c2g__codajournal__c as je on je.c2g__transaction__c = tli.c2g__transaction__c
                 left join erp.public.chart_of_account as coa
                           on to_char(tli.general_ledger_account_number__c) = coa.old_account
                 left join erp.public.account_internal_ids as aid
                           on to_char(aid.account) = coa.new_account
        where (tli.c2g__linereference__c like '%Collection%'
            or tli.c2g__linedescription__c like '%Collection%')
          and (tli.c2g__linereference__c not like '%Loan Collection%'
            or tli.c2g__linedescription__c not like '%Loan Collection%')
          and transaction_date__c = '{created_date}'
          and (general_ledger_account_number__c = '2103'
            or (general_ledger_account_number__c = '1008'
                and tli.c2g__linereference__c not like '%EQB Transfer%')
            or general_ledger_account_number__c = '1115'
            or general_ledger_account_number__c = '1116')
        group by 1, 2, 3, 4, 5, 9, 10
        order by 8 desc
        """

    # Manual Payment (assure that we don't capture EQB transactions)
    # Loan Collection TC and Ario
    # Funding Loan
    # NSF's
    # Loan Interest Accrual / Loan Interest Adjustments
    # Facility Management (EQB) Transactions
    # Push Funding MCAs
    # Credit Genie
