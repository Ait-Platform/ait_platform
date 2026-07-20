from flask_wtf import FlaskForm
from wtforms import (DateField, HiddenField, IntegerField, SelectField,
    StringField, BooleanField, 
    TextAreaField, SubmitField, 
    SelectMultipleField
)
from wtforms.validators import DataRequired, Length, Optional, URL, Email
from flask_wtf.file import FileAllowed, MultipleFileField
from flask_wtf.file import FileAllowed, FileRequired, FileField


class DummyForm(FlaskForm):
    """Empty form used only for CSRF protection."""
    pass

class RoleForm(FlaskForm):
    pass

class EnrollmentStep1Form(FlaskForm):
    full_name = StringField("Stage Name / Pseudonym", validators=[DataRequired()])
    dob = DateField("Date of Birth", format="%Y-%m-%d", validators=[DataRequired()])
    parent_email = StringField("Parent/Guardian Email", validators=[Optional(), Email()])
    submit = SubmitField("Continue to Talent Selection")

class EnrollmentStep2Form(FlaskForm):
    gender = SelectField("Gender", choices=[
        ("male","Male"),
        ("female","Female"),
        ("other","Other")
    ], validators=[DataRequired()])

    city = StringField("City", validators=[Optional()])

    province = StringField("Province", validators=[Optional()])

    #address_line = StringField("Address", validators=[DataRequired()])
    #occupation = StringField("Occupation", validators=[DataRequired()])
    #highest_qualification = StringField("Highest Qualification", validators=[DataRequired()])
    submit = SubmitField("Continue")

class EnrollmentStep3Form(FlaskForm):
    pledge = BooleanField("I agree to the pledge", validators=[DataRequired()])
    submit = SubmitField("Finish")

class UpdateBiodataForm(FlaskForm):
    full_name = StringField("Stage Name / Pseudonym", validators=[DataRequired()])
    dob = DateField("Date of Birth", format="%Y-%m-%d", validators=[DataRequired(message="Please provide your date of birth.")])
    gender = SelectField("Gender", choices=[
        ("male","Male"),
        ("female","Female"),
        ("other","Other")
    ], validators=[DataRequired()])
    parent_email = StringField("Parent/Guardian Email", validators=[Optional()])
    submit = SubmitField("Save Changes")

class BiodataForm(FlaskForm):
    full_name = StringField("Full Name", validators=[DataRequired()])
    id_number = StringField("ID Number", validators=[DataRequired()])
    dob = DateField("Date of Birth", validators=[Optional()])
    phone = StringField("Phone", validators=[Optional()])

    employer = StringField("Employer/Institution", validators=[Optional()])
    emergency_contact = StringField("Emergency Contact", validators=[Optional()])
    next_of_kin = StringField("Next of Kin", validators=[Optional()])

    pledge_signed = BooleanField("I pledge to uphold Cultural Fire values")
    signature = StringField("Signature", validators=[Optional()])

    submit = SubmitField("Save & Continue")

    age = IntegerField("Age", validators=[Optional()])
    grade = StringField("Grade", validators=[Optional()])
    school = StringField("School", validators=[Optional()])

    next_of_kin = StringField("Next of Kin", validators=[Optional()])
    employer_details = StringField("Employer Details", validators=[Optional()])

class TalentSubmissionForm(FlaskForm):
    category = StringField("Category", validators=[DataRequired(), Length(max=100)])
    media_url = StringField("Video or Audio Link", validators=[Optional(), URL()])
    description = TextAreaField("Description", validators=[Optional(), Length(max=500)])
    submit = SubmitField("Submit Talent")

class GroupCreateForm(FlaskForm):
    # CSRF token is automatically included by FlaskForm
    name = StringField("Group Name", validators=[DataRequired()])
    leader_id = SelectField("Leader", coerce=int, validators=[DataRequired()])
    members = SelectMultipleField("Members", coerce=int)
    submit = SubmitField("Create Group")

class NewGroupForm(FlaskForm):
    group_name = StringField("Group Name", validators=[DataRequired()])
    member_ids = SelectMultipleField("Choose Members", coerce=int, validators=[DataRequired()])
    submit = SubmitField("Create Group")

class UpdateGroupForm(FlaskForm):
    #group_name = StringField("Group Name", validators=[DataRequired()])
    member_ids = SelectMultipleField("Choose Members", coerce=int, validators=[DataRequired()])
    submit = SubmitField("Update Group")
    group_name = StringField("Group Name", validators=[DataRequired(message="Please enter a group name.")])

class TalentForm(FlaskForm):
    talent_name = StringField("Talent Name", validators=[DataRequired()])
    custom_talent = StringField("Custom Talent")
    category_item_id = SelectField("Category", coerce=int)
    #video_url = StringField("Video URL")
    segment_id = SelectField("Segment", choices=[], coerce=int)  # ✅ add this
    submit = SubmitField("Update Talent")
    

    talent_files = MultipleFileField(
        "Upload Files",
        validators=[FileAllowed(["mp4", "mov", "avi"])]
    )

class ParentAddParticipantForm(FlaskForm):
    child_id = SelectField("Select Child", validators=[DataRequired()])
    relationship = SelectField("Relationship", choices=[("Parent","Parent"),("Guardian","Guardian")], validators=[DataRequired()])
    consent = BooleanField("Consent", validators=[DataRequired()])
    submit = SubmitField("Link Participant")

class ParentDashboardForm(FlaskForm):
    """Empty form, only used for CSRF protection on dashboard actions."""
    pass

class PermissionForm(FlaskForm):
    child_id = HiddenField()
    item_id = HiddenField()
    item_type = HiddenField()
    action = HiddenField()
    submit = SubmitField("Submit")

# forms.py
class SponsorForm(FlaskForm):
    participant_id = SelectField("Participant", coerce=int)
    show_id = SelectField("Show", coerce=int)
    item_id = SelectField("Sponsorship Item", coerce=int)
    submit = SubmitField("Create Sponsorship")

class SupporterForm(FlaskForm):
    participant_id = SelectField("Participant", choices=[], coerce=int)
    amount = IntegerField("Amount")
    duration_months = IntegerField("Duration (months)")
    note = StringField("Note")

    # New referee fields

    # Supporter type
    
    supporter_type = SelectField(
        "Supporter Type",
        choices=[
            ("financial", "Financial"),
            ("volunteer", "Volunteer"),
            ("mentor", "Mentor"),
            ("other", "Other")
        ],
        validators=[DataRequired()]
    )
    supporter_type_other = StringField("If Other, please specify")
    
    # Availability and stipend
    availability = StringField("Availability")  # could be a text field or custom widget
    stipend_required = BooleanField("Stipend Required")
    referee_id = SelectField("Referee", coerce=int, validators=[DataRequired()])

class TalentDetailsForm(FlaskForm):
    context = SelectField("Context", choices=[], coerce=int)
    sponsor_id = SelectField("Sponsor", choices=[], coerce=int)
    #sponsor_type = SelectField("Sponsorship Type", choices=[])
    supporter_id = SelectField("Supporter", choices=[], coerce=int)
    #supporter_type = SelectField("Support Type", choices=[])
    submit = SubmitField("Save Details")

class ShowcaseForm(FlaskForm):
    title = StringField(
        "Title",
        validators=[DataRequired(), Length(min=2, max=120)]
    )
    description = TextAreaField(
        "Description",
        validators=[Length(max=500)]
    )
    start_date = DateField(
        "Start Date",
        validators=[DataRequired()],
        format="%Y-%m-%d"
    )
    end_date = DateField(
        "End Date",
        format="%Y-%m-%d"
    )
    location = StringField(
        "Location",
        validators=[DataRequired(), Length(min=2, max=255)]
    )
    submit = SubmitField("Create Show")

class SegmentSelectForm(FlaskForm):
    segment_id = SelectField("Segment", coerce=int, validators=[DataRequired()])
    submit = SubmitField("Continue")



class PageantForm(FlaskForm):
    segment_id = SelectField("Segment", choices=[], coerce=int, validators=[DataRequired()])
    video_file = FileField("Upload Video", validators=[
        FileRequired(),
        FileAllowed(["mp4", "mov", "avi"], "Video files only!")
    ])
    submit = SubmitField("Submit Video")







