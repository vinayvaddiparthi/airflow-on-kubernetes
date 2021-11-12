select fields:lookup_key::varchar as lookup_key
from ZETATANGO.KYC_PRODUCTION.ENTITIES_BUSINESS_REPORTS
where fields:report_type::varchar in ('equifax_business_default', 'equifax_personal_default')
    and lookup_key not in (
        select lookup_key
        from ZETATANGO.KYC_PRODUCTION.RAW_BUSINESS_REPORT_RESPONSES
    )