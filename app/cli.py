# app/cli.py
import click
from flask.cli import with_appcontext
from app.services.visitors_report import send_daily_visitors_report

@click.command("send-visitors-report")
@with_appcontext
def send_visitors_report_cmd():
    send_daily_visitors_report()

def register_cli(app):
    app.cli.add_command(send_visitors_report_cmd)
