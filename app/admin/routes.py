from app.admin.security import dashboard
from flask import (
    Blueprint, current_app, render_template, 
    redirect, url_for, abort, request, session,
    jsonify, flash)
from app.models.loss import LcaResult
from app.utils import reading_utils
from app.utils.roles import is_admin  # reuse your helper
from app.extensions import db
from . import admin_bp
from sqlalchemy import select, func, text
from flask_login import current_user
import uuid
from app.models.payment import VoucherToken
from app.models.auth import AuthSubject

