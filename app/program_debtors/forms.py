from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed
from wtforms import StringField, TextAreaField, IntegerField, FloatField, SelectField, DateField, SubmitField, BooleanField
from wtforms.validators import DataRequired, Email, Optional

class SoaProfileForm(FlaskForm):
    # This form is now just for global settings
    submit = SubmitField("Save Global Settings")

class SenderProfileForm(FlaskForm):
    business_name = StringField("Business Name", validators=[DataRequired()])
    address = TextAreaField("Address", validators=[Optional()])
    phone = StringField("Phone", validators=[Optional()])
    email = StringField("Email", validators=[Optional(), Email()])
    logo_file = FileField("Upload Logo (Optional)", validators=[
        FileAllowed(['jpg', 'png', 'jpeg'], 'Images only!')
    ])
    is_default = BooleanField("Set as Default Profile", default=False)
    submit = SubmitField("Save Sender Profile")

class DebtorForm(FlaskForm):
    name = StringField("SOA Client Name / Company", validators=[DataRequired()])
    email = StringField("Email Address", validators=[Optional(), Email()])
    phone = StringField("Phone Number", validators=[Optional()])
    apply_interest = BooleanField("Apply Monthly Arrears Interest", default=True)
    sender_profile_id = SelectField("Sender Profile (Business Details)", coerce=int, validators=[Optional()])
    bank_account_id = SelectField("Assigned Bank Account", coerce=int, validators=[Optional()])
    submit = SubmitField("Save Setup")

class OpeningBalanceForm(FlaskForm):
    opening_balance = FloatField("Opening Balance", validators=[DataRequired()])
    txn_date = DateField("Date", validators=[DataRequired()])
    submit = SubmitField("Set Balance")

class RecurringChargeForm(FlaskForm):
    charge_description = StringField("Recurring Charge Description", validators=[DataRequired()])
    charge_amount = FloatField("Charge Amount", validators=[DataRequired()])
    charge_frequency = SelectField("Frequency", choices=[
        ('monthly', 'Monthly'),
        ('weekly', 'Weekly'),
        ('once', 'Once-off')
    ])
    day_of_month = IntegerField("Day (1-31)", validators=[Optional()], default=1)
    start_date = DateField("Start Date (Optional)", validators=[Optional()])
    end_date = DateField("End Date (Optional)", validators=[Optional()])
    submit = SubmitField("Add Charge")

class TransactionForm(FlaskForm):
    txn_date = DateField("Transaction Date", validators=[DataRequired()])
    description = StringField("Description", validators=[DataRequired()])
    kind = SelectField("Type", choices=[('debit', 'Debit (Invoice/Charge)'), ('credit', 'Credit (Payment/Receipt)')], validators=[DataRequired()])
    amount = FloatField("Amount", validators=[DataRequired()])
    ref = StringField("Reference (Optional)", validators=[Optional()])
    submit = SubmitField("Add Transaction")

class BankAccountForm(FlaskForm):
    bank_name = StringField("Bank Name", validators=[DataRequired()])
    account_name = StringField("Account Name", validators=[DataRequired()])
    account_number = StringField("Account Number", validators=[DataRequired()])
    bsb_branch = StringField("Branch Code / BSB", validators=[Optional()])
    swift_code = StringField("SWIFT Code", validators=[Optional()])
    is_default = BooleanField("Set as Default Account", default=False)
    submit = SubmitField("Save Bank Account")
