"""Conservative municipal PDF extraction. Evidence is never inferred from filenames."""
from datetime import date
from decimal import Decimal
from hashlib import sha256
from io import BytesIO
import re
from pypdf import PdfReader
from .annual_tariff_register import component, comparable_unit, token
from app.utils.tariff_versions import resolve, identity
from .municipal_tariff_sources import official_url, year_start

VERSION = 'ethekwini-tables-v1'
REQUIRED = tuple('W0'+str(i) for i in range(1,6))+tuple('S0'+str(i) for i in range(1,6))+('ELEC01','W_LOSS','W_MGT','S_REFUSE','W_SURCHARGE','S_SURCHARGE')
LOWER=(0,201,834,1001,1501)
UPPER=(200,833,1000,1500,None)
WIDTH=('0.2','0.633','0.167','0.5','999')
FRACTIONS=('0.95','0.75','0.75','0.65','0.60')
CATEGORY='Domestic; no individual break-pressure tank; valuation above R350000; development not exceeding two storeys; no special agreement'


def compact(text):
    return ' '.join(text.split())


def adopted(text, year):
    """Require a self-specific adoption statement, exact year and annual dates.

    General budget approval announcements do not bind a particular rate table.
    Unsupported council-resolution formats remain exceptions for investigation.
    """
    start=year_start(year);flat=compact(text)
    pattern=(r'These (?:municipal )?tariffs for '+str(start)+r'[/ -]'+str(start+1)+
             r' were adopted by (?:the )?eThekwini (?:Municipal )?Council on (\d{4}-\d{2}-\d{2})')
    found=re.findall(pattern,flat,re.I)
    annual=re.search(r'effective from 1 July '+str(start)+r' (?:to|until) 30 June '+str(start+1),flat,re.I)
    if len(set(found))!=1 or not annual:return None
    try:when=date.fromisoformat(found[0])
    except ValueError:return None
    if when>date.today():return None
    if re.search(r'\b(draft|subject to approval|not adopted)\b',flat[:1500],re.I):return None
    return dict(date=when.isoformat(),statement=re.search(pattern,flat,re.I).group(0))


def column(text, year):
    """Reviewed five-column layout, excluding VAT column for requested year."""
    first=year_start(year)
    expression=(r'Present Tariff Excluding Vat \('+str(first-1)+'/'+str(first)+r'\) '+
                r'Proposed Tariff Excluding Vat \('+str(first)+'/'+str(first+1)+r'\) '+
                r'Proposed Tariff Including Vat \('+str(first)+'/'+str(first+1)+r'\)')
    return bool(re.search(expression,compact(text),re.I))


def five_rates(text):
    # Full numeric columns, no percentages, quantities, rounded/invented rates.
    rows=re.findall(r'(?<![\d.])((?:\d{1,3}(?:,\d{3})*|\d+)\.\d{2,4})\s+((?:\d{1,3}(?:,\d{3})*|\d+)\.\d{2,4})\s+((?:\d{1,3}(?:,\d{3})*|\d+)\.\d{2,4})\s+((?:\d{1,3}(?:,\d{3})*|\d+)\.\d{2,4})\s+((?:\d{1,3}(?:,\d{3})*|\d+)\.\d{2,4})(?![\d.])',text)
    return [tuple(value.replace(',','') for value in row) for row in rows]


def extract(data, metadata):
    if sha256(data).hexdigest()!=metadata['sha256']:raise ValueError('Source checksum mismatch.')
    official_url(metadata['source_url'],pdf=True)
    year=metadata['financial_year'];year_start(year)
    reader=PdfReader(BytesIO(data))
    if reader.is_encrypted or not 0<len(reader.pages)<=2000:raise ValueError('Unsupported source PDF.')
    pages=[page.extract_text() or '' for page in reader.pages]
    adoption=adopted('\n'.join(pages),year)
    output={};issues=[];observations=[]
    for number,text in enumerate(pages,1):
        flat=compact(text)
        if re.search(r'electricity|refuse',flat,re.I):
            for line in text.splitlines():
                if re.search(r'kWh|per month|fixed charge',line,re.I) and re.search(r'\d+\.\d{2,4}',line):
                    observations.append(dict(page=number,text=line[:500],status='HOLD: category, VAT basis and calculation mapping require verification'))
        if not column(text,year):continue
        def retain(code, section, utility, unit, category=CATEGORY, supported=True):
            rates=five_rates(section)
            if len(rates)!=1:return
            if code.startswith(('W0','S0')):
                i=int(code[-1])-1
                bounds=((r'up to 6',),(r'greater than 6',r'up to 25'),(r'greater than 25',r'up to 30'),(r'greater than 30',r'up to 45'),(r'greater than 45',))[i]
                if not all(re.search(bound,compact(section),re.I) for bound in bounds):return
            row=dict(code=code,rate=rates[0][1],unit=unit,utility_type=utility,category=category,
                     effective_date=str(year_start(year))+'-07-01',source_url=metadata['source_url'],
                     sha256=metadata['sha256'],page=number,excerpt=compact(section)[:1000],supported=supported)
            if re.fullmatch('[SW]0[1-5]',code):
                i=int(code[-1])-1;row.update(block_start=LOWER[i],block_end=UPPER[i],reduction_factor=WIDTH[i],discharge=FRACTIONS[i] if code[0]=='S' else None)
            else:row.update(block_start=0,block_end=0,reduction_factor='1')
            output.setdefault(code,[]).append(row)
        if 'CCDOMFPCC' in flat and 'without the intervention of individual break pressure tanks' in flat:
            section=flat.split('without the intervention of individual break pressure tanks',1)[1]
            patterns=[r'(CCDOMFPRC \(b\).*?greater than R350 000.*?)(?=CCDOMFPCC)',
                      r'CCDOMFPCC\s*\(c\)(.*?)(?=CCDOMFPCC)',r'CCDOMFPCC\s*\(d\)(.*?)(?=CCDOMFPCC)',
                      r'CCDOMFPCC\s*\(e\)(.*?)(?=CCDOMFPCC)',r'CCDOMFPCC\s*\(f\)(.*?)(?=For all other)']
            for i,pattern in enumerate(patterns):
                found=re.findall(pattern,section,re.I)
                if len(found)==1:retain('W0'+str(i+1),found[0],'water','kL')
        if 'CCSWDOM<=2' in flat and 'without the intervention of individual break pressure' in flat:
            chunks=flat.split('CCSWDOM<=2')[1:]
            if len(chunks)==5:
                for i,chunk in enumerate(chunks):retain('S0'+str(i+1),chunk,'sewerage','kL')
        for code,anchor,kind in (('W_SURCHARGE','Water infrastructure surcharge per kl/month.','water_surcharge'),('S_SURCHARGE','Sewage disposal infrastructure surcharge per kl/month','sewerage_surcharge')):
            if anchor.lower() in flat.lower():
                section=re.split(re.escape(anchor),flat,flags=re.I)[1]
                match=five_rates(section)
                if match:retain(code,' '.join(match[0]),kind,'R/kL',kind.replace('_',' '))
        if 'Water loss charge for under ground leaks for individual dwellings' in flat:
            section=flat.split('Water loss charge for under ground leaks for individual dwellings')[1].split('3. Connection')[0]
            retain('W_LOSS',section,'water_fixed','fixed','Water loss levy ? monthly basis requires verification',False)
        if 'CCMGMT' in flat:
            section=flat.split('CCMGMT',1)[1].split('WATS0089CC')[0]
            retain('W_MGT',section,'water_fixed','fixed','Monthly distribution charge per sub meter',False)
    # Fractions must be explicitly in the same official publication. This parser
    # does not reinterpret percentages or legacy reduction_factor as band widths.
    discharge_page=next((n for n,text in enumerate(pages,1) if 'development does not exceed' in compact(text) and re.findall(r'(\d+)%',text)==['65','60','90','90','95','75','75']),None)
    for code,rows in output.items():
        if code.startswith('S0'):
            for row in rows:row['supported']=bool(discharge_page);row['discharge_page']=discharge_page
    if not adoption:issues.append('HOLD: official adoption of these specific annual tariff tables is not confirmed.')
    if metadata.get('publication_status') in ('DRAFT','PROPOSED'):
        adoption=None;issues.append('HOLD: publication is explicitly draft/proposed.')
    return dict(year=year,source=metadata,adoption=adoption,extracted=output,issues=issues,parser=VERSION,observations=observations[:100])


def compare(extraction, stored, evidence):
    """All supported rates compared exactly. No proximity tolerance or writes."""
    year=extraction['year'];start=date(year_start(year),7,1)
    groups={};rows=[]
    for old in stored:
        code=component(old,(evidence or {}).get(old['id'],{})) or 'UNSUPPORTED:'+str(old.get('code'))
        groups.setdefault(code,[]).append(old)
    for code in sorted(set(REQUIRED)|set(groups)|set(extraction['extracted'])):
        versions=groups.get(code,[]);selected,problems=resolve(versions,start)
        values=extraction['extracted'].get(code,[]);candidate=values[0] if len(values)==1 else None
        reasons=list(problems)
        if len(values)!=1:reasons.append('Required tariff absent, duplicated or unsupported in extracted official table.')
        if candidate and not candidate['supported']:reasons.append('Calculation/category mapping is unsupported; existing treatment is preserved.')
        if len({identity(v) for v in versions})>1:reasons.append('Multiple stored tariff identities map to this component.')
        old=selected[0] if len(selected)==1 else None
        if candidate and old:
            unit=str(old.get('unit') or '').strip()
            if unit and (not comparable_unit(code,unit,old) or code in ('W_MGT','S_REFUSE')):reasons.append('Stored unit or allocation basis conflicts with extracted unit.')
            prior=(evidence or {}).get(old['id'],{})
            if prior.get('municipality') and token(prior['municipality']) not in ('ethekwini','ethekwinimunicipality'):reasons.append('Stored municipality conflicts.')
            if prior.get('municipal_category') and code.startswith(('W0','S0')) and not token(prior['municipal_category']).startswith('domestic'):reasons.append('Stored consumer category conflicts.')
            if code.startswith(('W0','S0')) and Decimal(str(old.get('block_start') or 0))!=candidate['block_start']:reasons.append('Stored band boundary conflicts.')
        if not extraction['adoption']:reasons.append('Adoption not verified.')
        if evidence is None:reasons.append('Existing tariff verification storage is unavailable.')
        classification='EXCEPTION' if reasons else 'NEW' if old is None else 'UNCHANGED' if Decimal(str(old['rate']))==Decimal(candidate['rate']) else 'UPDATED'
        rows.append(dict(code=code,classification=classification,status='HOLD' if reasons else 'VERIFIED',candidate=candidate,existing=old,reasons=reasons))
    return dict(year=year,source=extraction['source'],adoption=extraction['adoption'],rows=rows,issues=extraction['issues'],parser=VERSION,observations=extraction.get('observations',[]))
