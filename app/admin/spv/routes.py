from flask import (
    Blueprint, flash, redirect, session as flask_session, 
    abort, render_template, current_app, make_response, url_for
    )
from sqlalchemy import text as sa_text
from app.admin.spv.forms import SpvAssetForm, SpvSectionForm
from app.extensions import db
from datetime import datetime
from flask import render_template
from flask_login import login_required
from app.models.spv import SpvAsset, SpvDeal, SpvSection
from . import spv_admin_bp
from werkzeug.utils import secure_filename
import os
from slugify import slugify


@spv_admin_bp.route("/")
@login_required
def spv_dashboard():

    return render_template(
        "admin/spv/investments.html"
    )

@spv_admin_bp.route("/sections")
@login_required
def section_list():

    sections = SpvSection.query.order_by(
        SpvSection.sort_order.asc()
    ).all()

    return render_template(
        "admin/spv/section_list.html",
        sections=sections
    )

@spv_admin_bp.route(
    "/sections/create",
    methods=["GET", "POST"]
)
@login_required
def create_section():

    form = SpvSectionForm()

    form.deal_id.choices = [
        (d.id, d.title)
        for d in SpvDeal.query.order_by(
            SpvDeal.title.asc()
        ).all()
    ]

    if form.validate_on_submit():

        #from slugify import slugify

        section = SpvSection(
            deal_id=form.deal_id.data,

            title=form.section_type.data
                .replace("-", " ")
                .title(),

            slug=slugify(form.section_type.data),

            content=form.content.data,

            sort_order=form.sort_order.data or 0
        )

        db.session.add(section)
        db.session.commit()

        flash(
            "Section created successfully.",
            "success"
        )

        return redirect(
            url_for(
                "spv_admin_bp.section_list"
            )
        )

    return render_template(
        "admin/spv/section_form.html",
        form=form
    )

@spv_admin_bp.route("/assets")
@login_required
def asset_list():

    assets = SpvAsset.query.order_by(
        SpvAsset.created_at.desc()
    ).all()

    return render_template(
        "admin/spv/asset_list.html",
        assets=assets
    )

@spv_admin_bp.route(
    "/assets/create",
    methods=["GET", "POST"]
)
@login_required
def create_asset():

    form = SpvAssetForm()

    form.section_id.choices = [
        (s.id, f"{s.deal.title} — {s.title}")
        for s in SpvSection.query.order_by(
            SpvSection.title.asc()
        ).all()
    ]

    if form.validate_on_submit():

        file_path = None

        if form.file.data:

            file = form.file.data

            filename = secure_filename(
                file.filename
            )

            upload_folder = os.path.join(
                "app",
                "static",
                "uploads",
                "spv"
            )

            os.makedirs(
                upload_folder,
                exist_ok=True
            )

            save_path = os.path.join(
                upload_folder,
                filename
            )

            file.save(save_path)

            file_path = (
                f"uploads/spv/{filename}"
            )

        last_sort = db.session.query(
            db.func.max(SpvAsset.sort_order)
        ).filter_by(
            section_id=form.section_id.data
        ).scalar()

        next_sort = (last_sort or 0) + 1

        asset = SpvAsset(
            section_id=form.section_id.data,
            title=form.title_type.data,
            asset_type=form.asset_type.data,
            file_path=file_path,
            external_url=form.external_url.data,
            sort_order=next_sort
        )

        db.session.add(asset)
        db.session.commit()

        flash(
            "Asset uploaded successfully.",
            "success"
        )

        return redirect(
            url_for(
                "spv_admin_bp.asset_list"
            )
        )

    return render_template(
        "admin/spv/asset_form.html",
        form=form
    )

@spv_admin_bp.route(
    "/sections/<int:section_id>/edit",
    methods=["GET", "POST"]
)
@login_required
def edit_section(section_id):

    section = SpvSection.query.get_or_404(section_id)

    form = SpvSectionForm(obj=section)

    form.deal_id.choices = [
        (d.id, d.title)
        for d in SpvDeal.query.order_by(
            SpvDeal.title
        ).all()
    ]

    if form.validate_on_submit():

        section.deal_id = form.deal_id.data

        section.section_type = (
            form.section_type.data
        )

        section.title = (
            form.section_type.data
            .replace("-", " ")
            .title()
        )

        section.slug = slugify(
            form.section_type.data
        )

        section.content = form.content.data

        section.sort_order = (
            form.sort_order.data or 0
        )

        db.session.commit()

        flash(
            "Section updated successfully.",
            "success"
        )

        return redirect(
            url_for("spv_admin_bp.section_list")
        )

    return render_template(
        "admin/spv/section_form.html",
        form=form,
        section=section,
        title="Edit Section"
    )

@spv_admin_bp.route(
    "/assets/edit/<int:asset_id>",
    methods=["GET", "POST"]
)
@login_required
def edit_asset(asset_id):

    asset = SpvAsset.query.get_or_404(asset_id)

    form = SpvAssetForm(obj=asset)

    form.section_id.choices = [
        (s.id, f"{s.deal.title} → {s.title}")
        for s in SpvSection.query
            .join(SpvDeal)
            .order_by(
                SpvDeal.title,
                SpvSection.sort_order
            )
            .all()
    ]

    if form.validate_on_submit():

        asset.section_id = form.section_id.data

        #asset.title = form.title.data
        asset.title = form.title_type.data

        asset.asset_type = form.asset_type.data

        asset.external_url = form.external_url.data

        if form.file.data:

            file = form.file.data

            filename = secure_filename(file.filename)

            upload_dir = os.path.join(
                current_app.root_path,
                "static",
                "uploads",
                "spv"
            )

            os.makedirs(upload_dir, exist_ok=True)

            file_path = os.path.join(
                upload_dir,
                filename
            )

            file.save(file_path)

            asset.file_path = f"uploads/spv/{filename}"

        db.session.commit()

        flash(
            "Asset updated successfully.",
            "success"
        )

        return redirect(
            url_for("spv_admin_bp.asset_list")
        )

    return render_template(
        "admin/spv/asset_form.html",
        form=form,
        asset=asset,
        page_title="Edit Asset"
    )

from flask import abort, send_from_directory
from werkzeug.utils import secure_filename
import os


@spv_admin_bp.route("/asset/view/<int:asset_id>")
@login_required
def view_asset(asset_id):

    asset = SpvAsset.query.get_or_404(asset_id)

    upload_folder = current_app.config["SPV_UPLOAD_FOLDER"]

    if not asset.file_name:
        abort(404)

    file_path = os.path.join(upload_folder, asset.file_name)

    if not os.path.exists(file_path):
        abort(404)

    return send_from_directory(
        upload_folder,
        asset.file_name,
        as_attachment=False
    )

