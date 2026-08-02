# app/cli.py
import click
from flask.cli import with_appcontext
from app.services.visitors_report import send_daily_visitors_report
from app.jobs.debtors_jobs import run_debtors_billing_job

@click.command("send-visitors-report")
@with_appcontext
def send_visitors_report_cmd():
    send_daily_visitors_report()

@click.command("run-debtors-billing")
@with_appcontext
def run_debtors_billing_cmd():
    run_debtors_billing_job()

def register_cli(app):
    app.cli.add_command(send_visitors_report_cmd)
    app.cli.add_command(run_debtors_billing_cmd)
