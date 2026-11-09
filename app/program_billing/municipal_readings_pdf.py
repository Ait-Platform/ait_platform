"""Read-only municipal submission of recorded portfolio readings."""
from io import BytesIO
from decimal import Decimal
from xml.sax.saxutils import escape
from flask import abort,request,send_file
from flask_login import current_user
from sqlalchemy import text,inspect
from app.extensions import db
from .metsoa_setup_safety import property_scope
from .municipal_metsoa import month_context


def report(property_id,period):
    property_scope(property_id)
    month,start,end=month_context(period)
    reference_available=any(c['name']=='municipal_reference' for c in inspect(db.session.connection()).get_columns('bil_meter'))
    reference_column='m.municipal_reference' if reference_available else 'CAST(NULL AS text)'
    # Ownership is enforced for every source row and every possible account link.
    reading_sql="""SELECT p.id AS property_id,p.name AS property_name,
        m.id AS meter_id,m.meter_number,REFERENCE_COLUMN AS municipal_reference,m.municipal_bill_number,
        m.utility_type,r.id AS reading_id,r.reading_date,r.reading_value
        FROM bil_property p JOIN bil_meter m ON m.property_id=p.id
        JOIN bil_meter_reading r ON r.meter_id=m.id
        WHERE p.manager_id=:user AND r.reading_date BETWEEN :start AND :end
        ORDER BY p.name,p.id,m.meter_number,m.id,r.reading_date,r.id"""
    meters=db.session.execute(text(reading_sql.replace('REFERENCE_COLUMN',reference_column)),
        dict(user=current_user.id,start=start,end=end)).mappings().all()
    accounts=db.session.execute(text("""SELECT a.id,a.property_id,a.account_number,a.water_meter_id,a.elec_meter_id
        FROM bil_muni_account a JOIN bil_property p ON p.id=a.property_id
        WHERE p.manager_id=:user ORDER BY a.id"""),dict(user=current_user.id)).mappings().all()
    grouped={'electricity':[],'water':[]}
    for row in meters:
        utility=(row['utility_type'] or '').lower()
        utility='electricity' if utility=='electrical' else utility
        if utility not in grouped:
            abort(409,description='A recorded portfolio reading has an unsupported utility type. Verify its meter setup before generating the municipal PDF.')
        linked=[a for a in accounts if a['property_id']==row['property_id'] and
                (row['meter_id'] in (a['water_meter_id'],a['elec_meter_id']) or
                 (row['municipal_bill_number'] and row['municipal_bill_number']==a['account_number']))]
        if len(linked)!=1 or row['municipal_bill_number'] not in (None,'',linked[0]['account_number']):
            abort(409,description='A recorded portfolio meter has a missing, conflicting or ambiguous municipal account association. Verify Municipal Accounts before generating the PDF.')
        value=Decimal(str(row['reading_value']))
        if not value.is_finite() or value<0:abort(409,description='A recorded reading needs review before municipal submission.')
        grouped[utility].append(dict(row,account_number=linked[0]['account_number'],reading=format(value,'f')))
    return dict(month=month,groups=grouped)


def presentation(data):
    """Shared display metadata; never replace differing saved reading dates."""
    from datetime import datetime
    try:month_label=datetime.strptime(data['month'],'%Y-%m').strftime('%B %Y')
    except ValueError:month_label=data['month']
    dates={row['reading_date'] for rows in data['groups'].values() for row in rows}
    common_date=next(iter(dates)) if len(dates)==1 else None
    date_label=(str(common_date.day)+' '+common_date.strftime('%B %Y')) if common_date else None
    return dict(month_label=month_label,common_date=common_date,reading_date_label=date_label,has_readings=bool(dates))


def render_pdf(data):
    from pathlib import Path
    import reportlab
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,Flowable,CondPageBreak
    fonts=Path(reportlab.__file__).parent/'fonts'
    # Bundled fonts keep print output consistent on Windows and Render.
    if 'MunicipalVera' not in pdfmetrics.getRegisteredFontNames():
        pdfmetrics.registerFont(TTFont('MunicipalVera',str(fonts/'Vera.ttf')))
        pdfmetrics.registerFont(TTFont('MunicipalVeraBold',str(fonts/'VeraBd.ttf')))
    display=presentation(data)
    normal=ParagraphStyle('body',fontName='MunicipalVera',fontSize=9,leading=11)
    heading=ParagraphStyle('heading',parent=normal,fontName='MunicipalVeraBold',fontSize=11,leading=14,spaceBefore=7,spaceAfter=4,keepWithNext=False)
    small=ParagraphStyle('small',parent=normal,fontSize=7.5,leading=10)
    def paragraph(value,style=normal):return Paragraph(escape(str(value or '')),style)
    class Checkbox(Flowable):
        def __init__(self):super().__init__();self.width=14;self.height=14
        def draw(self):self.canv.setLineWidth(.7);self.canv.rect(0,0,14,14,stroke=1,fill=0)
    class Acknowledgement(Flowable):
        def wrap(self,width,height):
            self.width=width;self.height=196;return self.width,self.height
        def draw(self):
            c=self.canv;c.setStrokeColor(colors.HexColor('#555555'));c.setLineWidth(.6)
            c.setFont('MunicipalVeraBold',10)
            c.drawString(0,180,'MUNICIPAL ACKNOWLEDGEMENT OF RECEIPT')
            c.setFont('MunicipalVera',10)
            for label,y in [('Name:',150),('Signature:',118),('Date:',86)]:
                c.drawString(0,y,label);c.line(65,y-2,255,y-2)
            c.drawString(285,150,'Municipal Stamp:')
            c.rect(285,12,self.width-285,128,stroke=1,fill=0)
    buffer=BytesIO()
    document=SimpleDocTemplate(buffer,pagesize=A4,rightMargin=16*mm,leftMargin=16*mm,topMargin=16*mm,bottomMargin=16*mm,title='Municipal Meter Readings - '+data['month'],author='Property Management')
    story=[Paragraph('MUNICIPAL METER READINGS',ParagraphStyle('title',parent=heading,fontSize=16,leading=20,spaceBefore=0)),
           paragraph('Reporting Month: '+display['month_label'])]
    if display['reading_date_label']:story.append(paragraph('Reading Date: '+display['reading_date_label']))
    elif display['has_readings']:story.append(paragraph('Reading dates vary; shown with each reading.',small))
    story.append(Spacer(1,3*mm))
    for utility,title in [('electricity','Electricity Readings (kWh)'),('water','Water Readings (kL)')]:
        story.extend([CondPageBreak(100),paragraph(title,heading)])
        rows=[[paragraph(label) for label in ['Account','Reference','Meter','Reading','Captured']]]
        for row in data['groups'][utility]:
            reading=paragraph(row['reading']) if display['common_date'] else Paragraph(escape(row['reading'])+'<br/><font size="7">'+row['reading_date'].isoformat()+'</font>',normal)
            rows.append([paragraph(row['account_number']),paragraph(row['municipal_reference']),paragraph(row['meter_number']),reading,Checkbox()])
        if len(rows)==1:rows.append([paragraph('No recorded readings for this period.'),'','','',''])
        table=Table(rows,colWidths=[document.width*.22,document.width*.23,document.width*.22,document.width*.21,document.width*.12],repeatRows=1,hAlign='LEFT')
        commands=[('GRID',(0,0),(-1,-1),.4,colors.HexColor('#888888')),('BACKGROUND',(0,0),(-1,0),colors.HexColor('#eeeeee')),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),('ALIGN',(-1,1),(-1,-1),'CENTER')]
        if len(rows)==2 and not data['groups'][utility]:commands.append(('SPAN',(0,1),(-1,1)))
        table.setStyle(TableStyle(commands));story.extend([table,Spacer(1,2*mm)])
    story.append(Acknowledgement())
    def footer(canvas,doc):
        canvas.setFont('MunicipalVera',7)
        canvas.drawString(16*mm,9*mm,'Municipal Meter Readings | '+data['month'])
        canvas.drawRightString(A4[0]-16*mm,9*mm,'Page '+str(doc.page))
    document.build(story,onFirstPage=footer,onLaterPages=footer)
    return buffer.getvalue()


def page(property_id):
    if len(request.args.getlist('billing_month'))>1:abort(400)
    data=report(property_id,request.args.get('billing_month'))
    response=send_file(BytesIO(render_pdf(data)),mimetype='application/pdf',as_attachment=True,download_name='Municipal-Meter-Readings-'+data['month']+'.pdf')
    response.headers['Cache-Control']='private, no-store'
    return response
