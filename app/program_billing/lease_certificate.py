"""Approved Word certificate: pure snapshot mapping, never HTML reconstruction."""

from copy import deepcopy
from decimal import Decimal

from hashlib import sha256

from io import BytesIO

from pathlib import Path

import json,re

from zipfile import ZipFile,ZIP_DEFLATED

from lxml import etree as ET



W='http://schemas.openxmlformats.org/wordprocessingml/2006/main'

MASTER=Path(__file__).with_name('private_templates')/'AIT_Lease_Certificate_Master.docx'

MASTER_SHA256='5e0987df79cc9e47a92aed3dc26c867cb0b958a8b17cf9cb7dc2b1f4bd52107f'

TOKEN=re.compile(r'\[[A-Z][A-Z0-9_]*\]')

LEGACY={'LEASE_REFERENCE':'s1-01','OCCUPATION_DATE':'s1-19','RENEWAL_PROVISION':'s1-26',

'KEY_DEPOSIT_PARTICULARS':'s1-30','AGREEMENT_PREPARATION_FEE_PARTICULARS':'s1-31',

'PAYMENT_DAY_PARTICULARS':'s1-25','PAYMENT_METHOD_REFERENCE':'s1-37','VAT_TREATMENT':'s1-34',

'RATES_INSURANCE_BASELINES':'s1-35','DEPOSIT_DEADLINE_PARTICULARS':'s1-29'}

PREMISES=('property_name','property_address','lease_scope','leased_portion_description',

'property_extent_sqm','leased_area_sqm','unit_count','property_legal_description','excluded_areas',

'plan_a_identifier','plan_b_identifier')

AMOUNTS=('rental_adjustments_text','parking_other_charges_text','utilities_allocation_text','monthly_rent','monthly_rent_words','rent_effective_date','deposit_amount',

'parking_applicability','parking_details','parking_bay_count','parking_rent_per_bay')+tuple(

'adjustment_'+str(i)+'_'+field for i in (1,2,3) for field in ('applicability','date','amount'))

BANK_FIELDS=('bank_name','branch_name','branch_code','account_type','account_holder','account_number')





def load_master():

    data=MASTER.read_bytes()

    if sha256(data).hexdigest()!=MASTER_SHA256:raise ValueError('Certificate master integrity check failed')

    return data





def present(value):

    return value is not None and str(value).strip()!='' and not re.search(r'\[[^\]]+\]',str(value))





def selected_tenants(items):
    """The selected array is authoritative, including an explicitly empty selection."""
    parties=items.get('_parties',{})
    if 'lessees' in parties:
        return deepcopy(parties['lessees']) if isinstance(parties['lessees'],list) else []
    legacy=parties.get('lessee')
    return [deepcopy(legacy)] if isinstance(legacy,dict) and legacy else []



def compatible_items(items):

    result=deepcopy(items);tenants=selected_tenants(result)

    if tenants:

        result.setdefault('_parties',{})['lessees']=tenants

        result['_parties']['lessee']=deepcopy(tenants[0])

    return result





def authority_status(items,key,party,represented,has_data):

    saved=items.get('_certificate_authorities',{}).get(key,items.get('_ait_standard',{}).get('authorities',{}).get(key,{}))

    if not has_data:return {}

    expected=sha256(json.dumps({'representative':party,'represented':represented},

        sort_keys=True,separators=(',',':'),ensure_ascii=False,default=str).encode()).hexdigest()

    confirmed=(saved.get('confirmed') is True and present(saved.get('evidence')) and

        present(party.get('representative')) and present(party.get('capacity')) and present(party.get('identity_number')) and saved.get('party_fingerprint')==expected)

    return {'confirmed':'Confirmed' if confirmed else 'Unconfirmed','evidence':saved.get('evidence','')}





def bank_snapshot(items):
    """Only explicit selections, including an explicit cleared selection, are used."""
    saved=items.get('_certificate_banking',{})
    if saved:return saved
    legacy=items.get('_banking_utilities',{})
    if legacy.get('schema_version')==2 and legacy.get('property_id'):
        return legacy.get('property_accounts',{}).get('rental',{})
    return {}


def values(items,property_record=None,owners=(),tenants=()):

    """Read snapshots; authorized registers only fill explicitly selected identities."""

    items=compatible_items(items);result={}

    def put(name,value):

        if present(value):result['['+name+']']=str(value).strip()

    for name,key in LEGACY.items():put(name,items.get(key,{}).get('value'))

    premise=items.get('_premises',{})

    for key in PREMISES:put(key.upper(),premise.get(key))

    if property_record:

        put('PROPERTY_NAME',premise.get('property_name') or property_record.get('name'))

        put('PROPERTY_ADDRESS',premise.get('property_address') or property_record.get('address'))

    period=items.get('_period',{})

    for key,legacy in (('commencement_date','s1-18'),('expiry_date','s1-20'),('duration_months','s1-17')):

        value=period.get(key)

        if not present(value):value=items.get(legacy,{}).get('value')

        if key=='duration_months' and present(value):

            match=re.fullmatch(r'(\d+)\s*(months?|years?)?',str(value).strip(),re.I)

            value=str(int(match[1])*(12 if (match[2] or '').lower().startswith('year') else 1)) if match else None

        put(key.upper(),value)

    put('RENEWAL_NOTICE_DATE',period.get('renewal_notice_date'))

    for key in AMOUNTS:put(key.upper(),items.get('_amounts',{}).get(key))

    banking=bank_snapshot(items)

    if banking.get('bank_detail_id'):

        for key in BANK_FIELDS:put(key.upper(),banking.get(key))

    utilities=items.get('_banking_utilities',{})

    put('UTILITY_ALLOCATION_METHOD',utilities.get('utility_allocation_method'))

    if utilities.get('utility_reference_mode')=='provided':put('UTILITY_REFERENCES',utilities.get('utility_references'))

    elif utilities.get('utility_reference_mode')=='none':put('UTILITY_NO_REFERENCES_REASON',utilities.get('utility_no_references_reason'))

    lessors=items.get('_contracting_lessors',{}).get('identities',[])

    by_owner={str(p['owner_id']):p for p in owners if p.get('owner_id') is not None}

    for index,party in enumerate(lessors,1):

        registered=by_owner.get(str(party.get('owner_id')),{})

        for key in ('name','identity_number'):put('LESSOR_'+str(index)+'_'+key.upper(),party.get(key) or registered.get(key))

    parties=items.get('_parties',{});lessor=parties.get('lessor',{})

    for key in ('signatory_name','signatory_identity_number','signatory_capacity'):put('LESSOR_'+key.upper(),lessor.get(key))

    p={'key':'landlords','name':'All contracting landlords','representative':lessor.get('signatory_name',''),

       'identity_number':lessor.get('signatory_identity_number',''),'capacity':lessor.get('signatory_capacity','')}

    for key,value in authority_status(items,'landlords',p,lessors,bool(lessors or lessor)).items():

        put('LESSOR_AUTHORITY_'+key.upper(),value)

    rows=selected_tenants(items);by_tenant={str(p['id']):p for p in tenants if p.get('id') is not None}

    for index,party in enumerate(rows,1):

        registered=by_tenant.get(str(party.get('tenant_id')),{})

        for key in ('legal_name','identity_number','signatory_name','signatory_identity_number','signatory_capacity'):

            put('LESSEE_'+str(index)+'_'+key.upper(),party.get(key) or registered.get(key))

        p={'key':'tenant-'+str(index-1),'name':party.get('legal_name',''),'representative':party.get('signatory_name',''),

           'identity_number':party.get('signatory_identity_number',''),'capacity':party.get('signatory_capacity','')}

        for key,value in authority_status(items,p['key'],p,rows,bool(party)).items():

            put('LESSEE_'+str(index)+'_AUTHORITY_'+key.upper(),value)

    return result





MONEY_KEYS=('KEY_DEPOSIT_PARTICULARS','AGREEMENT_PREPARATION_FEE_PARTICULARS','MONTHLY_RENT','DEPOSIT_AMOUNT','PARKING_RENT_PER_BAY','ADJUSTMENT_1_AMOUNT','ADJUSTMENT_2_AMOUNT','ADJUSTMENT_3_AMOUNT')


def money_amount(raw):
    raw=str(raw).strip()
    if not re.fullmatch(r'-?[0-9]{1,12}(?:\.[0-9]{1,2})?',raw):
        raise ValueError('Enter a rand amount with at most two decimal places, without symbols or separators.')
    return Decimal(raw).quantize(Decimal('0.01'))


def money_words(raw):
    value=money_amount(raw);amount=abs(value)
    ones='zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen'.split()
    tens='zero ten twenty thirty forty fifty sixty seventy eighty ninety'.split()
    def small(n):
        if n<20:return ones[n]
        if n<100:return tens[n//10]+('-'+ones[n%10] if n%10 else '')
        return ones[n//100]+' hundred'+(' and '+small(n%100) if n%100 else '')
    def integer(n):
        if n<1000:return small(n)
        for size,label in ((10**9,'billion'),(10**6,'million'),(1000,'thousand')):
            if n>=size:
                whole,rest=divmod(n,size)
                return small(whole)+' '+label+((' and ' if rest<100 else ' ')+integer(rest) if rest else '')
    whole=int(amount);cents=int((amount-whole)*100)
    return ('minus ' if value<0 else '')+integer(whole)+' rand and '+integer(cents)+(' cent' if cents==1 else ' cents')


def document_values(items,property_record=None,owners=(),tenants=()):
    """Figures and words at render time only; no contractual arithmetic or writes."""
    result=values(items,property_record,owners,tenants)
    for name in MONEY_KEYS:
        token='['+name+']'
        if token not in result:continue
        try:
            raw=result[token];amount=money_amount(raw);words=money_words(raw)
        except ValueError:continue  # Unverified legacy text is retained verbatim.
        figures=('-' if amount<0 else '')+'R'+format(abs(amount),',.2f')
        result[token]=figures if name=='MONTHLY_RENT' else figures+' ('+words+')'
        if name=='MONTHLY_RENT':result['[MONTHLY_RENT_WORDS]']='('+words+')'
    return result



def xml(payload):

    with ZipFile(BytesIO(payload)) as z:

        return ET.fromstring(z.read('word/document.xml'),ET.XMLParser(resolve_entities=False,no_network=True))





def replace(root,mapping):

    """Replace across split Word runs; retain paragraph/run formatting."""

    for p in root.iter('{'+W+'}p'):

        nodes=list(p.iter('{'+W+'}t'));text=''.join(n.text or '' for n in nodes)

        offsets=[];position=0

        for n in nodes:offsets.append(position);position+=len(n.text or '')

        for match in reversed(list(TOKEN.finditer(text))):

            if match[0] not in mapping:continue

            affected=[i for i,start in enumerate(offsets) if start<match.end() and start+len(nodes[i].text or '')>match.start()]

            first,last=affected[0],affected[-1]

            before=(nodes[first].text or '')[:match.start()-offsets[first]]

            after=(nodes[last].text or '')[match.end()-offsets[last]:]

            nodes[first].text=before+mapping[match[0]]+(after if first==last else '')

            for i in affected[1:]:nodes[i].text=after if i==last else ''





def repeat(root,tag,count,prefix):

    ns={'w':W}

    outer=next(n for n in root.iter('{'+W+'}sdt') if n.find('./w:sdtPr/w:tag',ns) is not None

               and n.find('./w:sdtPr/w:tag',ns).get('{'+W+'}val')==tag)

    content=outer.find('{'+W+'}sdtContent');prototype=deepcopy(list(content)[0])

    for node in list(content):content.remove(node)

    for index in range(1,count+1):

        node=deepcopy(prototype)

        for t in node.iter('{'+W+'}t'):

            if t.text:t.text=re.sub(r'\['+prefix+r'_1_','['+prefix+'_'+str(index)+'_',t.text)

        if prefix=='LESSOR':

            row=node.find('.//{'+W+'}tr');next(row.iter('{'+W+'}t')).text='LESSORS — '+str(index)

        for control in node.iter('{'+W+'}id'):control.set('{'+W+'}val',str((73000 if prefix=='LESSOR' else 74000)+index))

        content.append(node)





def document(items=None,property_record=None,owners=(),tenants=()):

    items=items or {};payload=load_master();root=xml(payload)

    lessors=items.get('_contracting_lessors',{}).get('identities',[]);lessees=selected_tenants(items)

    repeat(root,'_contracting_lessors.identities',len(lessors) if lessors else 3,'LESSOR')

    repeat(root,'_parties.lessees',len(lessees) if lessees else 1,'LESSEE')

    for key,token in (('rental_adjustments_text','[ADJUSTMENT_1_APPLICABILITY]'),
                      ('parking_other_charges_text','[PARKING_APPLICABILITY]'),
                      ('utilities_allocation_text','[UTILITY_ALLOCATION_METHOD]')):
        narrative=items.get('_amounts',{}).get(key)
        if not present(narrative):continue
        paragraph=next(p for p in root.iter('{'+W+'}p') if token in ''.join(n.text or '' for n in p.iter('{'+W+'}t')))
        extra=deepcopy(paragraph);texts=list(extra.iter('{'+W+'}t'))
        texts[0].text='Additional particulars: '+str(narrative)
        for node in texts[1:]:node.text=''
        paragraph.addprevious(extra)
    replace(root,document_values(items,property_record,owners,tenants))

    for node in list(root.iter('{'+W+'}t')):
        if '\n' not in (node.text or ''):continue
        lines=node.text.split('\n');node.text=lines[0];cursor=node
        for line in lines[1:]:
            br=ET.Element('{'+W+'}br');cursor.addnext(br);cursor=br
            text=ET.Element('{'+W+'}t');text.text=line;cursor.addnext(text);cursor=text
    unresolved=sorted(set(TOKEN.findall(''.join(root.itertext()))))

    output=BytesIO()

    with ZipFile(BytesIO(payload)) as source,ZipFile(output,'w',ZIP_DEFLATED) as target:

        for entry in source.infolist():

            target.writestr(entry,ET.tostring(root,encoding='utf-8',xml_declaration=True)

                            if entry.filename=='word/document.xml' else source.read(entry.filename))

    return output.getvalue(),unresolved





def render_pdf(payload):

    from .lease_certificate_render import render_pdf as render

    return render(payload)
