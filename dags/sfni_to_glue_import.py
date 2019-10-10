import datetime
from typing import Set, Optional

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, select, text, column
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select, TableClause, ClauseElement


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def format_wide_table_select(
    table_name: str,
    engine: Engine,
    excluded_fields: Optional[Set[str]] = None,
    conditions: Optional[ClauseElement] = None,
):
    excluded_fields = excluded_fields or set()

    with engine.begin() as tx:
        stmt = Select(
            columns=[column("column_name")],
            from_obj=text("information_schema.columns"),
            whereclause=text(f"table_name = '{table_name}'"),
        )

        column_chunks = [
            x
            for x in chunks(
                [
                    column(x[0])
                    for x in tx.execute(stmt)
                    if x[0] not in {"id"}.union(excluded_fields)
                ],
                100,
            )
        ]

        tables = [
            select(
                [column("id")] + cols_,
                from_obj=TableClause(table_name, *([column("id")] + cols_)),
                whereclause=conditions,
            )
            for cols_ in column_chunks
        ]

        for i, table in enumerate(tables):
            table.schema = "salesforce"
            tables[i] = table.alias(f"t{i}")

        stmt = tables[0]

        for i in range(1, len(tables)):
            stmt = stmt.join(tables[i], tables[0].c.id == tables[i].c.id)

        stmt = select(
            [tables[0].c.id]
            + [
                col
                for table_ in tables
                for col in table_.c
                if col.name not in {"id"}.union(excluded_fields)
            ],
            from_obj=stmt,
        )

        return stmt

tables = ['acceptedeventrelation', 'account_product_eligibility__c', 'accountcleaninfo', 'accountcontactrelation', 'accountcontactrole', 'accountpartner', 'accountteammember', 'actionlinkgrouptemplate', 'actionlinktemplate', 'additionalnumber', 'agentwork', 'aggregateresult', 'announcement', 'apexclass', 'apexcomponent', 'apexlog', 'apexpage', 'apexpageinfo', 'apextestqueueitem', 'apextestresult', 'apextestresultlimits', 'apextestrunresult', 'apextestsuite', 'apextrigger', 'appdefinition', 'applicationcheck__c', 'appmenuitem', 'appraisal__c', 'approval__c', 'apptabmember', 'asset', 'assetrelationship', 'assettokenevent', 'assignment__c', 'assignmentrule', 'asyncapexjob', 'attachedcontentdocument', 'auradefinition', 'auradefinitionbundle', 'auradefinitionbundleinfo', 'auradefinitioninfo', 'authconfig', 'authconfigproviders', 'authsession', 'backgroundoperation', 'backgroundoperationresult', 'brandingset', 'brandingsetproperty', 'brandtemplate', 'businesshours', 'businessprocess', 'callcenter', 'campaign', 'campaign_promotions__mdt', 'campaignmember', 'campaignmemberstatus', 'case', 'casearticle', 'casecomment', 'casecontactrole', 'caseexternaldocument', 'casesolution', 'casestatus', 'casesubjectparticle', 'caseteammember', 'caseteamrole', 'caseteamtemplate', 'caseteamtemplatemember', 'caseteamtemplaterecord', 'categorynode', 'categorynodelocalization', 'changelock__c', 'chatter_delete_settings__c', 'chatteractivity', 'chatterextension', 'chatterextensionconfig', 'chatterextensionlocalization', 'checklist_criteria__c', 'checklist_item__c', 'checklist_template__c', 'clientbrowser', 'collaborationgroup', 'collaborationgroupmember', 'collaborationgroupmemberrequest', 'collaborationgrouprecord', 'collaborationinvitation', 'colordefinition', 'communication_consent__c', 'community', 'connectedapplication', 'contact', 'contactcleaninfo', 'contactrequest', 'contentasset', 'contentbody', 'contentdistribution', 'contentdistributionview', 'contentdocument', 'contentdocumentlink', 'contentfolder', 'contentfolderitem', 'contentfolderlink', 'contentfoldermember', 'contenthubitem', 'contenthubrepository', 'contentversion', 'contentworkspace', 'contentworkspacedoc', 'contentworkspacemember', 'contentworkspacepermission', 'contract', 'contractcontactrole', 'contractstatus', 'credit_bureau_settings__mdt', 'credit_check__c', 'cronjobdetail', 'crontrigger', 'custombrand', 'custombrandasset', 'customhttpheader', 'customobjectuserlicensemetrics', 'custompermission', 'custompermissiondependency', 'dashboard', 'dashboardcomponent', 'data_sync_field_mapping_setting__mdt', 'datacloudaddress', 'datacloudcompany', 'datacloudcontact', 'datacloudownedentity', 'datacloudpurchaseusage', 'dataintegrationrecordpurchasepermission', 'datasetexport', 'datasetexportevent', 'datasetexportpart', 'datastatistics', 'datatype', 'declinedeventrelation', 'delayed_days_mapping__mdt', 'document', 'document__c', 'documentattachmentmap', 'docusign_document_options__mdt', 'domain', 'domainsite', 'dsfs__docusign_envelope__c', 'dsfs__docusign_envelope_document__c', 'dsfs__docusign_envelope_recipient__c', 'dsfs__docusign_recipient_status__c', 'dsfs__docusign_status__c', 'dsfs__docusignaccountconfiguration__c', 'duplicatejob', 'duplicatejobdefinition', 'duplicatejobmatchingrule', 'duplicatejobmatchingruledefinition', 'duplicaterecorditem', 'duplicaterecordset', 'duplicaterule', 'emailmessage', 'emailmessagerelation', 'emailservicesaddress', 'emailservicesfunction', 'emailstatus', 'emailtemplate', 'embeddedservicedetail', 'entitydefinition', 'entityparticle', 'entitysubscription', 'event', 'eventbussubscriber', 'eventlogfile', 'eventrelation', 'externaldatasource', 'externaldatauserauth', 'externalsocialaccount', 'fairwarn__alert__c', 'fairwarn__alertticketassociation__c', 'fairwarn__investigation__c', 'fairwarn__involved_user__c', 'feedcomment', 'feeditem', 'feedlike', 'feedsignal', 'feedtrackedchange', 'fielddefinition', 'fieldpermissions', 'filesearchactivity', 'financial_account__c', 'fiscalyearsettings', 'flexqueueitem', 'flowinterview', 'flowrecordrelation', 'flowstagerelation', 'folder', 'folderedcontentdocument', 'footprint__all_reports__c', 'footprint__analysis_log__c', 'footprint__batch_parameter__mdt', 'footprint__foot_print_recordtype__c', 'footprint__footprint__c', 'footprint__footprint_allfields__c', 'footprint__footprint_allfu__c', 'footprint__footprint_allobjects__c', 'footprint__footprint_allwf__c', 'footprint__footprint_line_item__c', 'footprint__footprint_line_item_detail__c', 'footprint__footprint_listview__c', 'grantedbylicense', 'group', 'groupmember', 'holiday', 'icondefinition', 'idea', 'ideacomment', 'individual', 'industry_id__c', 'installedmobileapp', 'integration_error__c', 'knowledge__datacategoryselection', 'knowledge__ka', 'knowledge__kav', 'knowledge__viewstat', 'knowledge__votestat', 'knowledgeableuser', 'knowledgearticle', 'knowledgearticleversion', 'knowledgearticleviewstat', 'knowledgearticlevotestat', 'lead', 'lead_conversion_setting__mdt', 'leadcleaninfo', 'leadstatus', 'lightningexperiencetheme', 'lightningtogglemetrics', 'lightningusagebyapptypemetrics', 'lightningusagebybrowsermetrics', 'lightningusagebyflexipagemetrics', 'lightningusagebypagemetrics', 'linkedarticle', 'listemail', 'listemailrecipientsource', 'listview', 'listviewchart', 'listviewchartinstance', 'loan__c', 'loginevent', 'loginip', 'logouteventstream', 'lookedupfromactivity', 'macro', 'macroinstruction', 'mailmergetemplate', 'matchingrule', 'matchingruleitem', 'messagebrokersetting__c', 'milestone__c', 'milestones_creation_setting__mdt', 'mobileapplicationdetail', 'name', 'namedcredential', 'note', 'note__c', 'oauthtoken', 'objectpermissions', 'openactivity', 'opportunity__hd', 'opportunity_contact_role__c', 'opportunity_record_type_setting__mdt', 'opportunitycompetitor', 'opportunitycontactrole', 'opportunitylineitem', 'opportunitypartner', 'opportunitystage', 'opportunityteammember', 'order', 'orderitem', 'organization', 'orglifecyclenotification', 'orgwideemailaddress', 'outgoingemail', 'outgoingemailrelation', 'ownedcontentdocument', 'ownerchangeoptioninfo', 'partner', 'partner_id__c', 'partnerrole', 'payout__c', 'pendingservicerouting', 'period', 'permissionset', 'permissionsetassignment', 'permissionsetgroup', 'permissionsetgroupcomponent', 'permissionsetlicense', 'permissionsetlicenseassign', 'pi__asyncrequest__c', 'pi__asyncrequest_settings__c', 'pi__category_contact_score__c', 'pi__category_lead_score__c', 'pi__demo_settings__c', 'pi__engagecampaignrecipient__c', 'pi__ldfilter__c', 'pi__objectchangelog__c', 'pi__pardot_scoring_category__c', 'pi__pardottask__c', 'pi__partner_settings__c', 'pi__trigger_settings__c', 'picklistvalueinfo', 'platformaction', 'platformcachepartition', 'platformcachepartitiontype', 'presenceconfigdeclinereason', 'presencedeclinereason', 'presenceuserconfig', 'presenceuserconfigprofile', 'presenceuserconfiguser', 'pricebook2', 'pricebookentry', 'processdefinition', 'processinstance', 'processinstancenode', 'processinstancestep', 'processinstanceworkitem', 'processnode', 'processor_id__c', 'product2', 'product__c', 'product_summary__c', 'profile', 'publisher', 'pushtopic', 'queueroutingconfig', 'queuesobject', 'quicktext', 'quote', 'quote__c', 'quote__c_hd', 'quotedocument', 'quotelineitem', 'quotetemplaterichtextdata', 'recentlyviewed', 'recordaction', 'recordtype', 'recordtypelocalization', 'reference_data__c', 'registration__c', 'relationshipdomain', 'relationshipinfo', 'remuneration__c', 'report', 'samlssoconfig', 'scontrol', 'scontrollocalization', 'searchactivity', 'searchlayout', 'secureagentscluster', 'servicechannel', 'servicechannelstatus', 'servicepresencestatus', 'sessionpermsetactivation', 'setupaudittrail', 'setupentityaccess', 'site', 'sitedetail', 'socialpersona', 'socialpost', 'solution', 'solutionstatus', 'stamp', 'stampassignment', 'stamplocalization', 'staticresource', 'store_code__c', 'streamingchannel', 'subindustry_id__c', 'system_defaults__mdt', 'tabdefinition', 'talkdesk__cxmflow__c', 'talkdesk__talkdesk_activity__c', 'talkdesk__talkdesk_disposition_code__c', 'talkdesk__talkdesk_public__c', 'talkdesk__talkdesk_syncing_custom_object__c', 'task', 'taskpriority', 'taskstatus', 'tenantusageentitlement', 'testsuitemembership', 'thirdpartyaccountlink', 'todaygoal', 'topic', 'topicassignment', 'topiclocalization', 'trigger_setting__mdt', 'undecidedeventrelation', 'user', 'useraccountteammember', 'userappinfo', 'userappmenucustomization', 'userappmenuitem', 'userentityaccess', 'userfieldaccess', 'userlicense', 'userlistview', 'userlistviewcriterion', 'userlogin', 'userpermissionaccess', 'userpreference', 'userrecordaccess', 'userrole', 'userservicepresence', 'userteammember', 'verification__c', 'visibilityupdateevent', 'vote', 'waveautoinstallrequest', 'wavecompatibilitycheckitem', 'web_service_security__c', 'web_service_settings__c', 'work_case_creation_settings__mdt']

# wide_tables = [
#     {"name": "opportunity", "condition": None},
#     {"name": "pricing__c", "condition": None},
# ]


def ctas_to_glue(sobject: str):
    engine = create_engine(
        "presto://presto-production-internal.presto.svc:8080/sfni",
    )

    with engine.begin() as tx:
        tx.execute(f'''
        create table if not exists "glue"."sfni".{sobject} as select * from "sfni"."salesforce"."{sobject}"
        with no data
        ''').fetchall()

        try:
            max_date = tx.execute(f"select max(systemmodstamp) from glue.sfni.{sobject}").fetchall()[0][0]
            max_date = datetime.datetime.fromisoformat(max_date).__str__()
        except Exception:
            max_date = datetime.datetime.fromtimestamp(0).__str__()

        stmt = text(f'''
        insert into "glue"."sfni".{sobject} select * from "sfni"."salesforce"."{sobject}"
        where systemmodstamp > cast(:max_date as timestamp)
        ''').bindparams(max_date=max_date)

        tx.execute(stmt).fetchall()


def ctas_to_snowflake(sobject: str):
    engine = create_engine(
        "presto://presto-production-internal.presto.svc:8080/sfni",
    )

    with engine.begin() as tx:
        tx.execute(f'''
        create table if not exists "snowflake_sfni"."public".{sobject} as select * from "glue"."sfni"."{sobject}"
        with no data
        ''').fetchall()

        try:
            max_date = tx.execute(f'select max(systemmodstamp) from "snowflake_sfni"."public"."{sobject}"').fetchall()[0][0]
            max_date = datetime.datetime.fromisoformat(max_date).__str__()
        except Exception:
            max_date = datetime.datetime.fromtimestamp(0).__str__()

        stmt = text(f'''
        insert into "snowflake_sfni"."public"."{sobject}" select * from "glue"."sfni"."{sobject}"
        where systemmodstamp > cast(:max_date as timestamp)
        ''').bindparams(max_date=max_date)

        tx.execute(stmt).fetchall()


with DAG(
        "sfni_to_glue_import",
        start_date=datetime.datetime(2019,10,9),
        schedule_interval=None
) as dag:
    for t in tables:
        dag << PythonOperator(
            task_id=f"glue__{t}",
            python_callable=ctas_to_glue,
            op_kwargs={
                "sobject": t
            },
            pool="sfni_pool"
        ) >> PythonOperator(
            task_id=f"snowflake__{t}",
            python_callable=ctas_to_snowflake,
            op_kwargs={
                "sobject": t
            },
            pool="sfni_pool"
        )
