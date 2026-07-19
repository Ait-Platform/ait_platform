with open('app/program_tpx/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_header = """from flask import Blueprint, redirect, render_template, url_for, request, flash
from flask_login import login_required, current_user
from app.extensions import db
from app.models.tpx import TPXPassport, TPXEmployment, TPXQualification, TPXSkill
from .forms import CandidateProfileForm, WorkExperienceForm, EducationForm, SkillForm

tpx_bp = Blueprint(
    "tpx_bp",
    __name__,
    template_folder="templates"
)

@tpx_bp.route("/welcome")
def welcome():
    return render_template("program_tpx/welcome.html")
"""

if '@tpx_bp.route("/about")' in text:
    text = new_header + '\n' + text[text.find('@tpx_bp.route("/about")'):]

text = text.replace('TPXCandidate', 'TPXPassport')
text = text.replace('candidate_id', 'passport_id')
text = text.replace('candidate=', 'passport=')
text = text.replace('candidate.', 'passport.')
text = text.replace('if not candidate:', 'if not passport:')
text = text.replace('candidate = ', 'passport = ')

text = text.replace('TPXWorkExperience', 'TPXEmployment')
text = text.replace('TPXEducation', 'TPXQualification')

with open('app/program_tpx/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
