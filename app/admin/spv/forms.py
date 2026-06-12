from flask_wtf import FlaskForm
from wtforms import (
    StringField,
    TextAreaField,
    IntegerField,
    SelectField,
    SubmitField
)
from wtforms.validators import DataRequired
from flask_wtf.file import FileField
from flask_wtf.file import FileAllowed

class SpvSectionForm(FlaskForm):

    deal_id = SelectField(
        "Deal",
        coerce=int,
        validators=[DataRequired()]
    )

    section_type = SelectField(
        "Section",
        choices=[
            ("overview", "Overview"),
            ("investment-highlights", "Investment Highlights"),
            ("financials", "Financials"),
            ("property-details", "Property Details"),
            ("risk-factors", "Risk Factors"),
            ("exit-strategy", "Exit Strategy"),
            ("management", "Management"),
            ("legal", "Legal"),
            ("documents", "Documents"),
        ],
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

    title_type = SelectField(
        "Title",
        choices=[
            ("municipality-consent", "Municipality Consent"),
            ("zoning-certificate", "Zoning Certificate"),
            ("site-plan", "Site Plan"),
            ("architectural-plan", "Architectural Plan"),
            ("survey-diagram", "Survey Diagram"),
            ("title-deed", "Title Deed"),
            ("financial-model", "Financial Model"),
            ("valuation-report", "Valuation Report"),
            ("lease-agreement", "Lease Agreement"),
            ("shareholders-agreement", "Shareholders Agreement"),
            ("photo-gallery", "Photo Gallery"),
            ("property-images", "Property Images"),
            ("investment-memorandum", "Investment Memorandum"),
            ("bank-confirmation", "Bank Confirmation"),
        ],
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





