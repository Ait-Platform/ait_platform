"""Lossless paragraph/table content for saved Special Conditions."""
from markupsafe import Markup,escape
W='http://schemas.openxmlformats.org/wordprocessingml/2006/main'


def plain_text(blocks):
    return '\n'.join(block['text'] if block['kind']=='paragraph' else '\n'.join('\t'.join(row) for row in block['rows']) for block in blocks)


def legal_blocks(blocks):
    from .lease_document_format import legal_text
    result=[]
    for block in blocks:
        if block['kind']=='paragraph':result.append('<div>'+str(legal_text(block['text']))+'</div>')
        elif block['kind']=='table':
            table=['<table class="legal-utility-table"><thead>']
            for index,row in enumerate(block['rows']):
                if index==1:table.append('</thead><tbody>')
                table.append('<tr>');tag='th' if index==0 else 'td'
                table.extend('<'+tag+'>'+str(escape(cell)).replace('\n','<br>')+'</'+tag+'>' for cell in row);table.append('</tr>')
            table.append('</tbody></table>' if len(block['rows'])>1 else '</thead></table>');result.append(''.join(table))
        else:raise ValueError('Unsupported saved clause block.')
    return Markup(''.join(result))
