from flask_wtf import FlaskForm
from wtforms import DateField, SelectField, FloatField, TextAreaField, SubmitField, BooleanField
from wtforms.validators import DataRequired, Optional

class HcOnboardingForm(FlaskForm):
    dob = DateField('Date of Birth', validators=[Optional()])
    biological_sex = SelectField('Biological Sex', choices=[('', 'Select'), ('Male', 'Male'), ('Female', 'Female'), ('Other', 'Other')], validators=[Optional()])
    blood_type = SelectField('Blood Type', choices=[('', 'Select'), ('A+', 'A+'), ('A-', 'A-'), ('B+', 'B+'), ('B-', 'B-'), ('AB+', 'AB+'), ('AB-', 'AB-'), ('O+', 'O+'), ('O-', 'O-')], validators=[Optional()])
    height_cm = FloatField('Height (cm)', validators=[Optional()])
    weight_kg = FloatField('Weight (kg)', validators=[Optional()])
    chronic_conditions = TextAreaField('Chronic Conditions (if any)', validators=[Optional()])
    consent_ai = BooleanField('I agree to the AI processing of my health data', validators=[DataRequired(message="You must agree to continue.")])
    submit = SubmitField('Save Profile')
