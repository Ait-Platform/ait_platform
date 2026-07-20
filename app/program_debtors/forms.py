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
    name = StringField("Debtor Name / Company", validators=[DataRequired()])
    email = StringField("Email Address", validators=[Optional(), Email()])
    phone = StringField("Phone Number", validators=[Optional()])
    
    opening_balance = IntegerField("Opening Balance (in Cents, Optional)", validators=[Optional()], default=0)
    
    # Charge Map Fields
    charge_description = StringField("Recurring Charge Description (Optional)", validators=[Optional()])
    charge_amount = IntegerField("Charge Amount (in Cents)", validators=[Optional()], default=0)
    charge_frequency = SelectField("Frequency", choices=[
        ('monthly', 'Monthly'),
        ('weekly', 'Weekly'),
        ('once', 'Once-off')
    ], default='monthly')
    
    submit = SubmitField("Save Debtor")

class TransactionForm(FlaskForm):
    txn_date = DateField("Transaction Date", validators=[DataRequired()])
    description = StringField("Description", validators=[DataRequired()])
    kind = SelectField("Type", choices=[('debit', 'Debit (Invoice/Charge)'), ('credit', 'Credit (Payment/Receipt)')], validators=[DataRequired()])
    amount = IntegerField("Amount (in Cents)", validators=[DataRequired()])
    ref = StringField("Reference (Optional)", validators=[Optional()])
    submit = SubmitField("Add Transaction")
