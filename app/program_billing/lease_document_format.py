"""Safe display-only formatting of saved legal text; stored wording is untouched."""
import re
from markupsafe import Markup,escape


def inline(value):
    escaped=str(escape(str(value)))
    return re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',escaped)


def legal_text(value):
    lines=str(value).split('\n');output=[];index=0
    def cells(line):return [cell.strip() for cell in line.strip().strip('|').split('|')]
    while index<len(lines):
        line=lines[index]
        if index+1<len(lines) and '|' in line and re.fullmatch(r'\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*',lines[index+1]):
            headers=cells(line);table=['<table class="legal-utility-table"><thead><tr>']
            table.extend('<th>'+inline(c)+'</th>' for c in headers);table.append('</tr></thead><tbody>');index+=2
            while index<len(lines) and '|' in lines[index] and len(cells(lines[index]))==len(headers):
                table.append('<tr>');table.extend('<td>'+inline(c)+'</td>' for c in cells(lines[index]));table.append('</tr>');index+=1
            table.append('</tbody></table>');output.append(''.join(table));continue
        heading=re.match(r'^#{1,6}\s+(.+)$',line)
        output.append('<strong>'+inline(heading[1])+'</strong>' if heading else inline(line))
        index+=1
        if index<len(lines):output.append('<br>')
    return Markup(''.join(output))


def legal_heading(value):
    return legal_text(re.sub(r'^#{1,6}\s+','',str(value)).upper())


def display_name(prop):
    name=prop.get('name','')
    return 'Almond' if prop.get('id')==26 and name.casefold() in ('almond','almond house') else name


def internal_reference(value):
    return bool(re.search(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',str(value),re.I))
