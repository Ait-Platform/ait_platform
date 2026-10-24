"""Conservative display/save casing for human names and signing places only."""
import re

def human(value):
    def word(match):
        text=match.group()
        if text.isupper():return text
        if not text.islower() and any(char.isupper() for char in text[1:]):return text
        return text[:1].upper()+text[1:]
    return re.sub(r"[^\W\d_]+",word,str(value or ''),flags=re.UNICODE)

def party(record):
    result=dict(record)
    if result.get('party_type',result.get('type')) in ('natural_person','individual'):
        for key in ('legal_name','name'):
            if key in result:result[key]=human(result[key])
    for key in ('signatory_name','representative_name'):
        if key in result:result[key]=human(result[key])
    return result
