from flask_wtf import FlaskForm
from wtforms import (
    StringField,
    TextAreaField,
    IntegerField,
    SelectField,
    SubmitField,
    HiddenField
)
from wtforms.validators import DataRequired
from flask_wtf.file import FileField
from flask_wtf.file import FileAllowed

class SpvSectionForm(FlaskForm):

    deal_id = HiddenField(
        "Deal",
        validators=[DataRequired()]
    )

    title = StringField(
        "Section Title",
        validators=[DataRequired()]
    )

    content = TextAreaField(
        "Content",
        validators=[DataRequired()]
    )

    sort_order = IntegerField("Sort Order", default=0)

    submit = SubmitField("Save Section")

class SpvAssetForm(FlaskForm):

    section_id = SelectField(
        "Section",
        coerce=int,
        validators=[DataRequired()]
    )


    asset_type = SelectField(
        "Asset Type",
        choices=[
            ("image", "Image"),
            ("pdf", "PDF"),
            ("video", "Video"),
            ("map", "Map"),
            ("sketch", "Sketch")
        ]
    )

    file = FileField(
        "Upload File",
        validators=[
            FileAllowed(
                [
                    "jpg",
                    "jpeg",
                    "png",
                    "webp",
                    "pdf"
                ]
            )
        ]
    )

    external_url = StringField(
        "External URL"
    )

    submit = SubmitField(
        "Save Asset"
    )

class SpvDealForm(FlaskForm):
    title = StringField("Deal Title", validators=[DataRequired()])
    summary = TextAreaField("Summary", validators=[DataRequired()])
    status = SelectField(
        "Status",
        choices=[("open", "Open"), ("closed", "Closed")],
        default="open"
    )
    submit = SubmitField("Save Deal")






