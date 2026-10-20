"""Bounded native Word/LibreOffice conversion; never recreate Word tables in HTML."""
from io import BytesIO
from pathlib import Path
import os,shutil,subprocess,sys,tempfile,threading
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
_LOCK=threading.Lock()


class RenderUnavailable(RuntimeError):
    pass


def render_pdf(payload):
    with _LOCK,tempfile.TemporaryDirectory(prefix='ait-certificate-') as folder:
        root=Path(folder);source=root/'certificate.docx';source.write_bytes(payload)
        try:
            if sys.platform=='win32':
                script=Path(__file__).with_name('lease_certificate_render.ps1')
                subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass',
                    '-File',str(script),'-Folder',str(root)],check=True,capture_output=True,timeout=45)
                images=sorted(root.glob('page-*.png'),key=lambda p:int(p.stem.split('-')[-1]))
                if not images or len(images)>30:raise RenderUnavailable('Unsupported certificate page count')
                output=BytesIO();pdf=canvas.Canvas(output,pagesize=A4,pageCompression=1)
                pdf.setTitle('AIT Lease Certificate Draft')
                for image in images:
                    pdf.drawImage(str(image),0,0,width=A4[0],height=A4[1]);pdf.showPage()
                pdf.save();return output.getvalue()
            executable=os.getenv('PM_LEASE_SOFFICE_PATH') or shutil.which('soffice')
            if not executable:raise RenderUnavailable('A Word-compatible PDF renderer is not configured')
            profile=(root/'profile').as_uri()
            subprocess.run([executable,'-env:UserInstallation='+profile,'--headless','--convert-to','pdf',
                '--outdir',str(root),str(source)],check=True,capture_output=True,timeout=45,
                env=dict(os.environ,HOME=str(root),XDG_CACHE_HOME=str(root/'cache')))
            pdf=(root/'certificate.pdf').read_bytes()
            if not pdf.startswith(b'%PDF-'):raise RenderUnavailable('Invalid certificate render')
            return pdf
        except (OSError,subprocess.SubprocessError) as error:
            # Exception text/stdout may contain source paths or personal document text.
            raise RenderUnavailable('The Word certificate could not be rendered') from None
