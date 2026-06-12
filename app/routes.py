from flask import Response
from weasyprint import HTML

# app/routes.py
from flask import Response

def register_routes(app):
    @app.route("/pdf-test")
    def pdf_test():
        from app.utils.pdf_render import html_to_pdf_bytes
        
        html = """
        <html>
          <head><meta charset="utf-8"><title>PDF Test</title></head>
          <body style="font-family: sans-serif;">
            <h1 style="color:blue">Hello PDF</h1>
            <p>This PDF should open cleanly in Adobe.</p>
          </body>
        </html>
        """
        try:
            pdf = html_to_pdf_bytes(html)
        except Exception as e:
            return Response(f"PDF generation failed: {e}", status=500)

        return Response(
            pdf,
            mimetype="application/pdf",
            headers={
                "Content-Disposition": "attachment; filename=test.pdf",
                "Content-Length": str(len(pdf))
            }
        )
    

