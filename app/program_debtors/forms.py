from flask_wtf import FlaskForm
from wtforms import StringField, TextAreaField, IntegerField, SelectField, DateField, SubmitField
from wtforms.validators import DataRequired, Email, Optional

class SoaProfileForm(FlaskForm):
    business_name = StringField("Business Name", validators=[DataRequired()])
    address = TextAreaField("Address", validators=[Optional()])
    phone = StringField("Phone", validators=[Optional()])
    email = StringField("Email", validators=[Optional(), Email()])
    bank_details = TextAreaField("Bank Details (e.g. Account No, Branch)", validators=[Optional()])
    logo_url = StringField("Logo URL (Optional)", validators=[Optional()])
    submit = SubmitField("Save Profile")

class DebtorForm(FlaskForm):
    name = StringField("SOA Client Name / Company", validators=[DataRequired()])
    email = StringField("Email Address", validators=[Optional(), Email()])
    phone = StringField("Phone Number", validators=[Optional()])
    submit = SubmitField("Save Setup")

class OpeningBalanceForm(FlaskForm):
    opening_balance = IntegerField("Opening Balance (in Cents)", validators=[DataRequired()], default=0)
    txn_date = DateField("Date", validators=[DataRequired()])
    submit = SubmitField("Set Balance")

class RecurringChargeForm(FlaskForm):
    charge_description = StringField("Recurring Charge Description", validators=[DataRequired()])
    charge_amount = IntegerField("Charge Amount (in Cents)", validators=[DataRequired()], default=0)
    charge_frequency = SelectField("Frequency", choices=[
        ('monthly', 'Monthly'),
        ('weekly', 'Weekly'),
        ('once', 'Once-off')
    ], default='monthly')
    day_of_month = IntegerField("Day of Month (1-31)", validators=[Optional()], default=1)
    submit = SubmitField("Add Charge")

class TransactionForm(FlaskForm):
    txn_date = DateField("Transaction Date", validators=[DataRequired()])
    description = StringField("Description", validators=[DataRequired()])
    kind = SelectField("Type", choices=[('debit', 'Debit (Invoice/Charge)'), ('credit', 'Credit (Payment/Receipt)')], validators=[DataRequired()])
    amount = IntegerField("Amount (in Cents)", validators=[DataRequired()])
    ref = StringField("Reference (Optional)", validators=[Optional()])
    submit = SubmitField("Add Transaction")
