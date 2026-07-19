from flask_wtf import FlaskForm
from wtforms import StringField, TextAreaField, SelectField, SubmitField
from wtforms.validators import DataRequired, Length

class CandidateProfileForm(FlaskForm):
    first_name = StringField('First Name', validators=[DataRequired(), Length(max=100)])
    last_name = StringField('Last Name', validators=[DataRequired(), Length(max=100)])
    headline = StringField('Professional Headline', validators=[Length(max=255)])
    summary = TextAreaField('Summary')
    submit = SubmitField('Save Profile')
from wtforms.validators import Optional

class WorkExperienceForm(FlaskForm):
    job_title = StringField('Job Title', validators=[DataRequired(), Length(max=100)])
    company = StringField('Company Name', validators=[DataRequired(), Length(max=100)])
    location = StringField('Location', validators=[Length(max=100)])
    start_date = StringField('Start Date (e.g. MM/YYYY)', validators=[DataRequired(), Length(max=20)])
    end_date = StringField('End Date (or Present)', validators=[Length(max=20)])
    description = TextAreaField('Description')
    submit = SubmitField('Add Experience')

class EducationForm(FlaskForm):
    degree = StringField('Degree / Certification', validators=[DataRequired(), Length(max=100)])
    institution = StringField('Institution', validators=[DataRequired(), Length(max=100)])
    graduation_year = StringField('Graduation Year', validators=[Length(max=20)])
    submit = SubmitField('Add Education')

class SkillForm(FlaskForm):
    skill_name = StringField('Skill (e.g. Python, Agile, Welding)', validators=[DataRequired(), Length(max=50)])
    submit = SubmitField('Add Skill')
