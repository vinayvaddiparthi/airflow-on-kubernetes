def get_journal_entry_line(entry_type, account_internal_id, amount, created_date):
    return f"""
    <ns1:line>
         <ns1:account xsi:type='platformCore:RecordRef' internalId='{account_internal_id}'/>
         <ns1:{entry_type}>{amount}</ns1:{entry_type}>
         <ns1:memo xsi:type='xsd:string'>ETL-Test-{created_date}</ns1:memo>
    </ns1:line>
    """


def get_tran_date(date):
    if "T" not in date:
        date = date + "T00:00:00Z"
        print(f"fixed date: {date}")
    return f"""<ns1:tranDate xsi:type='xsd:dateTime'>{date}</ns1:tranDate>"""


def get_subsidiary(internal_id):
    return f"""<ns1:subsidiary xsi:type='platformCore:RecordRef' internalId='{internal_id}'/>"""


def get_journal_entry_line_list(lines):
    joined_line = "\n".join(lines)
    return f"""
    <ns1:lineList>
        {joined_line}
    </ns1:lineList>
    """


def get_journal_entry(subsidiary, tran_date, line_list):
    return f"""
    <platformMsg:record xsi:type='ns1:JournalEntry' xmlns:ns1='urn:general_2019_1.transactions.webservices.netsuite.com' xmlns:ns2='urn:core_2019_1.platform.webservices.netsuite.com'>
        {subsidiary}
        {tran_date}
        {line_list}
    </platformMsg:record>
    """


def get_body(method, email, password, account, app_id):
    return f"""
            <soapenv:Envelope xmlns:xsd='http://www.w3.org/2001/XMLSchema' xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>
                <soapenv:Header>
                    <platformMsg:preferences soapenv:mustUnderstand='0' soapenv:actor='http://schemas.xmlsoap.org/soap/actor/next' xmlns:platformMsg='urn:messages_2019_1.platform.webservices.netsuite.com'>
                        <platformMsg:useConditionalDefaultsOnAdd>true</platformMsg:useConditionalDefaultsOnAdd>
                        <platformMsg:ignoreReadOnlyFields>true</platformMsg:ignoreReadOnlyFields>
                        <platformMsg:warningAsError>false</platformMsg:warningAsError>
                    </platformMsg:preferences>
                    <ns2:applicationInfo soapenv:mustUnderstand='0' soapenv:actor='http://schemas.xmlsoap.org/soap/actor/next' xmlns:ns2='urn:messages_2019_1.platform.webservices.netsuite.com'>
                        <ns2:applicationId>{app_id}</ns2:applicationId>
                    </ns2:applicationInfo>
                    <passport soapenv:mustUnderstand='0' soapenv:actor='http://schemas.xmlsoap.org/soap/actor/next' xmlns='urn:core_2019_1.platform.webservices.netsuite.com'>
                        <email>{email}</email>
                        <password>{password}</password>
                        <account>{account}</account>
                    </passport>
                </soapenv:Header>
                <soapenv:Body>
                    {method}
                </soapenv:Body>
            </soapenv:Envelope>
            """


def add(obj):
    if isinstance(obj, list):
        payload = "\n".join(obj)
        return f"""
                            <platformMsg:addList xmlns:platformMsg='urn:messages_2019_1.platform.webservices.netsuite.com'>
                                {payload}
                            </platformMsg:addList>
            """
    else:
        return f"""
                    <platformMsg:add xmlns:platformMsg='urn:messages_2019_1.platform.webservices.netsuite.com'>
                        {obj}
                    </platformMsg:add>
    """


def search(obj):
    return f"""
                    <platformMsg:search xmlns:platformMsg='urn:messages_2019_1.platform.webservices.netsuite.com'>
                        {obj}
                    </platformMsg:search>
    """


def update(obj):
    if isinstance(obj, list):
        payload = "\n".join(obj)
        return f"""
                            <platformMsg:updateList xmlns:platformMsg='urn:messages_2019_1.platform.webservices.netsuite.com'>
                                {payload}
                            </platformMsg:updateList>
            """
    else:
        return f"""
                            <platformMsg:update xmlns:platformMsg='urn:messages_2019_1.platform.webservices.netsuite.com'>
                                {obj}
                            </platformMsg:update>
            """


def employee_search(attr, value):
    return f"""
                        <searchRecord xsi:type='q1:EmployeeSearch" xmlns:q1='urn:employees_2019_1.lists.webservices.netsuite.com'>
                            <q1:basic>
                                <{attr} operator='is' xmlns='urn:common_2019_1.platform.webservices.netsuite.com'>
                                    <searchValue xmlns='urn:core_2019_1.platform.webservices.netsuite.com'>{value}</searchValue>
                                </{attr}>
                            </q1:basic>
                        </searchRecord>
    """


def employee_search_reversed(attr, value):
    return f"""
                        <searchRecord xsi:type='q1:EmployeeSearch' xmlns:q1='urn:employees_2019_1.lists.webservices.netsuite.com'>
                            <q1:basic>
                                <{attr} operator='isNot' xmlns='urn:common_2019_1.platform.webservices.netsuite.com'>
                                    <searchValue xmlns='urn:core_2019_1.platform.webservices.netsuite.com'>{value}</searchValue>
                                </{attr}>
                            </q1:basic>
                        </searchRecord>
    """


def employee(first_name, last_name, gender, title, department, location, subsidiary, email):
    employee_record = [
        "<platformMsg:record xsi:type='ns1:Employee' xmlns:ns1='urn:employees_2019_1.lists.webservices.netsuite.com' xmlns:ns2='urn:core_2019_1.platform.webservices.netsuite.com'>"]
    if first_name:
        employee_record.append(f"<ns1:firstName xsi:type='xsd:string'>{escape(first_name)}</ns1:firstName>")
    if last_name:
        employee_record.append(f"<ns1:lastName xsi:type='xsd:string'>{escape(last_name)}</ns1:lastName>")
    if gender:
        # _omitted
        # _female
        # _male
        employee_record.append(
            f"<ns1:gender xsi:type='urn:types.employees.lists.webservices.netsuite.com'>{gender}</ns1:gender>")
    if title:
        employee_record.append(f"<ns1:title xsi:type='xsd:string'>{escape(title)}</ns1:title>")
    if department:
        employee_record.append(f"<ns1:firstName xsi:type='xsd:string'>{escape(first_name)}</ns1:firstName>")
    if location:
        employee_record.append(f"<ns1:firstName xsi:type='xsd:string'>{escape(first_name)}</ns1:firstName>")
    if subsidiary:
        employee_record.append(f"<ns1:subsidiary xsi:type='platformCore:RecordRef' internalId='{subsidiary}'/>")
    if email:
        employee_record.append(f"<ns1:email xsi:type='xsd:string'>{email}</ns1:email>")
    employee_record.append("</platformMsg:record>")
    r = "\n".join(employee_record)
    return f"""{r}"""


def escape(value):
    return value.replace('"', '*').replace("'", "*").replace("<", "-").replace(">", "-").replace("&", " and ")


def get_journal_entry_payload(created_date, rows, email, password, account, app_id):
    print('get_journal_entry_payload:')
    try:
        if rows[0]['subsidiary_id']:
            subsidiary = get_subsidiary(int(rows[0]['subsidiary_id']))
            tran_date = get_tran_date(rows[0]['tran_date'])
            lines = []
            for row in rows:
                if row['debit'] and float(row['debit']) > 0:
                    lines.append(get_journal_entry_line('debit', int(row['account_internal_id']), abs(row['debit']), created_date))
                if row['credit'] and float(row['credit']) < 0:
                    lines.append(get_journal_entry_line('credit', int(row['account_internal_id']), abs(row['credit']), created_date))
            journal_entry_line_list = get_journal_entry_line_list(lines)
            journal_entry = get_journal_entry(subsidiary, tran_date, journal_entry_line_list)
            return get_body(add(journal_entry), email, password, account, app_id)
    except Exception as e:
        print(f"Error: {rows}")
        print(e)


def search_employees(attr, value, email, password, account, app_id):
    return get_body(search(employee_search_reversed(attr, value)), email, password, account, app_id)
