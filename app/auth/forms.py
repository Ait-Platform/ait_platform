from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, IntegerField, SelectField, PasswordField, BooleanField
from wtforms.validators import InputRequired, Email, DataRequired, Length, Optional
from app.utils.country_list import COUNTRIES

class RegisterForm(FlaskForm):
    name = StringField("Full name", validators=[DataRequired()])
    email = StringField("Email", validators=[DataRequired(), Email()])
    # TEMP: allow 3 chars for testing
    password = PasswordField(
        "Password",
        validators=[DataRequired(), Length(min=8, message="Password must be at least 8 characters.")]
    )

    #country = SelectField(
    #    "Country (optional)",
    #    choices=[("", "Select a country")] + [(c, c) for c in COUNTRIES],
    #    validators=[Optional()],
    #)

    submit = SubmitField("Continue to Payment")

class ManagerPropertyForm(FlaskForm):
    property_name = StringField('Property Name', validators=[DataRequired()])
    location = StringField('Location')
    submit = SubmitField('Add Property')

class TenancyForm(FlaskForm):
    tenant_name = StringField('Tenant Name', validators=[DataRequired()])
    unit_number = StringField('Unit Number')
    duration_months = IntegerField('Duration (Months)')
    submit = SubmitField('Create Tenancy')

class LoginForm(FlaskForm):
    email = StringField("Email", validators=[DataRequired(), Email(), Length(max=255)])
    password = PasswordField("Password", validators=[DataRequired(), Length(min=8)])
    remember = BooleanField("Remember me")  # <-- add this
    submit = SubmitField("Sign in")
