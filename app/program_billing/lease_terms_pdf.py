"""Private development-reference PDF presentation; no legal text editing or writes."""
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import re
from xml.sax.saxutils import escape
from zipfile import ZipFile
from lxml import etree as ET
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen.canvas import Canvas
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image


def party_names(items):
    owners=items.get('_contracting_lessors',{}).get('identities',[])
    parties=items.get('_parties',{})
    tenants=parties.get('lessees',[]) or [parties.get('lessee',{})]
    return ([o.get('name') or '[LANDLORD NAME NOT SUPPLIED]' for o in owners] or ['[LANDLORD NAME NOT SUPPLIED]'],
        [t.get('legal_name') or '[TENANT NAME NOT SUPPLIED]' for t in tenants])


def initial_spaces(names,label):
    # Initial spaces acknowledge each contracting party, not a representative in
    # place of an owner. They do not assert that anybody has actually initialled.
    return [label+' '+str(i)+': '+(''.join(w[0].upper()+'.' for w in name.split()) if not name.startswith('[') else '[not supplied]')+'  __________'
        for i,name in enumerate(names,1)]


def reference_blocks(payload):
    """Retain reference tables and embedded plan/annexure images, in body order."""
    import posixpath
    from app.program_billing.lease_merge import W
    from app.program_billing import lease_preview as preview
    word='{'+W+'}';drawing='{http://schemas.openxmlformats.org/drawingml/2006/main}'
    relns='{http://schemas.openxmlformats.org/officeDocument/2006/relationships}'
    with ZipFile(BytesIO(payload)) as archive:
        root=ET.fromstring(archive.read('word/document.xml'),parser=ET.XMLParser(resolve_entities=False,no_network=True))
        relations={}
        if 'word/_rels/document.xml.rels' in archive.namelist():
            rels=ET.fromstring(archive.read('word/_rels/document.xml.rels'),parser=ET.XMLParser(resolve_entities=False,no_network=True))
            relations={r.get('Id'):r for r in rels}
        def walk(parent):
            result=[]
            for node in parent:
                if node.tag==word+'p':
                    text=preview.paragraph_text(node)
                    style=node.find('./'+word+'pPr/'+word+'pStyle')
                    name=style.get(word+'val','') if style is not None else ''
                    heading=bool(re.match(r'(?i)heading[0-9]+$',name) or name.lower()=='title')
                    drawings=list(node.iter(drawing+'blip'))
                    if drawings and text.strip():raise ValueError('Inline image/text layout requires reviewed rendering; no content was dropped.')
                    if node.find('./'+word+'pPr/'+word+'pageBreakBefore') is not None:result.append({'kind':'page_break'})
                    # Retain explicit source page breaks, not cached Word pagination.
                    segments=['']
                    for child in node.iter():
                        if child.tag==word+'t':segments[-1]+=child.text or ''
                        elif child.tag==word+'tab':segments[-1]+='\t'
                        elif child.tag==word+'br' and child.get(word+'type')=='page':segments.append('')
                        elif child.tag in (word+'br',word+'cr'):segments[-1]+='\n'
                    for index,value in enumerate(segments):
                        if index:result.append({'kind':'page_break'})
                        if value or not drawings:result.append({'kind':'heading' if heading else 'paragraph','text':value})
                    for image in drawings:
                        rel=relations.get(image.get(relns+'embed'))
                        if rel is None or rel.get('TargetMode')=='External':raise ValueError('Unresolved or external source image.')
                        target=posixpath.normpath(posixpath.join('word',rel.get('Target','')))
                        if not target.startswith('word/media/'):raise ValueError('Unsupported source image path.')
                        result.append({'kind':'image','data':archive.read(target)})
                    if node.find('.//'+word+'object') is not None or node.find('.//'+word+'pict') is not None:
                        raise ValueError('Legacy embedded objects require reviewed rendering; no content was dropped.')
                elif node.tag==word+'tbl':result.append({'kind':'table','rows':[[walk(c) for c in r.findall(word+'tc')] for r in node.findall(word+'tr')]})
                elif node.tag in (word+'sdt',word+'sdtContent',word+'customXml'):result.extend(walk(node))
                elif node.tag not in (word+'sectPr',word+'tcPr',word+'bookmarkStart',word+'bookmarkEnd'):raise ValueError('Unsupported reference body structure; no content was dropped.')
            return result
        return walk(root.find(word+'body'))


def build_pdf(payload,items,source):
    from app.program_billing import lease_standard as standard,lease_preview as preview
    from app.program_billing.lease_merge import W
    if sha256(payload).hexdigest()!=source['sha256']:raise ValueError('Terms source integrity check failed.')
    # Automatic Word numbering needs a numbering-aware renderer; never silently
    # omit or invent labels. Existing Almond source uses literal clause numbers.
    with ZipFile(BytesIO(payload)) as archive:
        root=ET.fromstring(archive.read('word/document.xml'),parser=ET.XMLParser(resolve_entities=False,no_network=True))
        if root.find('.//{'+W+'}numPr') is not None:
            raise ValueError('Automatic Word numbering requires reviewed PDF numbering support. No PDF was substituted.')
    merged=preview.merged_preview(standard.saved_particulars(items),payload,source['sha256'])
    blocks=reference_blocks(merged)
    import reportlab
    font_dir=Path(reportlab.__file__).parent/'fonts'
    for name,file in [('LeaseVera','Vera.ttf'),('LeaseVeraBold','VeraBd.ttf')]:
        if name not in pdfmetrics.getRegisteredFontNames():pdfmetrics.registerFont(TTFont(name,str(font_dir/file)))
    pdfmetrics.registerFontFamily('LeaseVera',normal='LeaseVera',bold='LeaseVeraBold',italic='LeaseVera',boldItalic='LeaseVeraBold')
    width,height=A4;left=right=19*mm;content_width=width-left-right
    base=ParagraphStyle('LeaseBody',fontName='LeaseVera',fontSize=9,leading=13,spaceAfter=6,splitLongWords=True)
    heading=ParagraphStyle('LeaseHeading',parent=base,fontName='LeaseVeraBold',fontSize=11,leading=15,spaceBefore=10,spaceAfter=8,keepWithNext=True)
    small=ParagraphStyle('LeaseMeta',parent=base,fontSize=7.3,leading=10,spaceAfter=2)
    foot=ParagraphStyle('LeaseFoot',parent=small,fontSize=6.8,leading=10)
    def markup(value):return escape(value).replace('\t','    ').replace('\n','<br/>') or '&#160;'
    def para(value,style=base):return Paragraph(markup(value),style)
    landlords,tenants=party_names(items)
    headers=[para('AGREEMENT OF LEASE',heading),para('Landlord(s): '+'; '.join(landlords),small),para('Tenant(s): '+'; '.join(tenants),small),
        para('DRAFT - DEVELOPMENT REFERENCE - NOT FOR SIGNATURE OR ISSUANCE',small)]
    header_height=sum(p.wrap(content_width,10000)[1]+p.style.spaceAfter for p in headers)+22*mm
    footer_rows=[]
    ls=initial_spaces(landlords,'Landlord');ts=initial_spaces(tenants,'Tenant')
    for i in range(max(len(ls),len(ts))):footer_rows.append([para(ls[i] if i<len(ls) else '',foot),para(ts[i] if i<len(ts) else '',foot)])
    footer=Table(footer_rows,colWidths=[content_width/2]*2)
    footer.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),4),('TOPPADDING',(0,0),(-1,-1),1),('BOTTOMPADDING',(0,0),(-1,-1),1)]))
    footer_height=footer.wrap(content_width,10000)[1]+19*mm
    if header_height+footer_height>height-60*mm:raise ValueError('Too many or overlong party names for a legible repeated header/footer. No names were omitted.')
    class NumberedCanvas(Canvas):
        def __init__(self,*args,**kwargs):super().__init__(*args,**kwargs);self.states=[]
        def showPage(self):self.states.append(dict(self.__dict__));self._startPage()
        def save(self):
            count=len(self.states)
            for state in self.states:
                self.__dict__.update(state);self.saveState();y=height-13*mm
                for p in headers:
                    _,h=p.wrap(content_width,10000);p.drawOn(self,left,y-h);y-=h+p.style.spaceAfter
                self.setStrokeColor(colors.HexColor('#b9c8d5'));self.line(left,y-3*mm,width-right,y-3*mm)
                footer.wrap(content_width,10000);footer.drawOn(self,left,14*mm)
                self.setFont('LeaseVera',7);self.setFillColor(colors.HexColor('#475569'))
                self.drawString(left,8*mm,'Source v'+str(source['version'])+' / '+source['sha256'][:12])
                self.drawRightString(width-right,8*mm,'Page '+str(self._pageNumber)+' of '+str(count))
                self.restoreState();Canvas.showPage(self)
            Canvas.save(self)
    def flow(block_list,available):
        result=[]
        for block in block_list:
            if block['kind']=='page_break':
                if result:result.append(PageBreak())
            elif block['kind']=='image':
                image=Image(BytesIO(block['data']))
                factor=min(1,available/image.imageWidth,(height-header_height-footer_height-15*mm)/image.imageHeight)
                image.drawWidth=image.imageWidth*factor;image.drawHeight=image.imageHeight*factor
                result.extend([image,Spacer(1,8)])
            elif block['kind']=='table':
                rows=block['rows']
                if not rows:continue
                columns=max(map(len,rows));cell_width=available/columns
                data=[[flow(cell,cell_width-10) or [para('')] for cell in row]+['']*(columns-len(row)) for row in rows]
                table=Table(data,colWidths=[cell_width]*columns,hAlign='LEFT',splitByRow=1,splitInRow=1)
                table.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('GRID',(0,0),(-1,-1),0.35,colors.HexColor('#cbd5e1')),('LEFTPADDING',(0,0),(-1,-1),5),('RIGHTPADDING',(0,0),(-1,-1),5),('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5)]))
                result.extend([table,Spacer(1,6)])
            else:
                value=block.get('text','')
                is_heading=block['kind']=='heading' or bool(re.match(r'^\d+\.?\s+[A-Z][A-Z /,&\-]+$',value.strip()))
                style=heading if is_heading else base
                number=re.match(r'^(\d+(?:\.\d+)+)\s',value)
                if number:style=ParagraphStyle('LeaseClause',parent=base,leftIndent=min(number[1].count('.')*5,20),firstLineIndent=0)
                result.append(para(value,style))
        return result
    stream=BytesIO()
    document=SimpleDocTemplate(stream,pagesize=A4,leftMargin=left,rightMargin=right,topMargin=header_height,bottomMargin=footer_height,
        title='Agreement of Lease - Core Terms Development Reference',author='AIT Platform',pageCompression=1)
    story=[para('AIT Core Terms and Conditions - development reference',heading),
        para('The complete pinned reference, including its schedules and signatures, is retained below. No clauses have been excluded or rewritten. Legal-release approval remains outstanding.',small)]
    story.extend(flow(blocks,content_width))
    document.build(story,canvasmaker=NumberedCanvas)
    return stream.getvalue()
