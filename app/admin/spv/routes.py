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


from flask import request

@spv_admin_bp.route("/")
@login_required
def spv_dashboard():
    deal_id = request.args.get("deal_id", type=int)
    section_id = request.args.get("section_id", type=int)
    
    deals = SpvDeal.query.order_by(SpvDeal.title.asc()).all()
    
    selected_deal = SpvDeal.query.get(deal_id) if deal_id else None
    
    if selected_deal and not section_id and selected_deal.sections:
        section_id = selected_deal.sections[0].id
        
    selected_section = SpvSection.query.get(section_id) if section_id else None
    
    asset_form = SpvAssetForm()
    
    return render_template(
        "admin/spv/unified_dashboard.html",
        deals=deals,
        selected_deal=selected_deal,
        selected_section=selected_section,
        asset_form=asset_form
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
    
    deal_id = request.args.get("deal_id", type=int)
    if request.method == "GET" and deal_id:
        form.deal_id.data = deal_id

    if form.validate_on_submit():

        from app.models.spv import SpvGenericSection
        title = form.title.data.title()
        generic_sec = SpvGenericSection.query.filter_by(title=title).first()
        if not generic_sec:
            db.session.add(SpvGenericSection(title=title))
            db.session.commit()

        section = SpvSection(
            deal_id=form.deal_id.data,

            title=title,

            slug=slugify(form.title.data),

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
                "spv_admin_bp.spv_dashboard",
                deal_id=section.deal_id,
                section_id=section.id
            )
        )

    from app.models.spv import SpvGenericSection
    generic_sections = SpvGenericSection.query.order_by(SpvGenericSection.title.asc()).all()

    return render_template(
        "admin/spv/section_form.html",
        form=form,
        generic_sections=generic_sections,
        deal_id=deal_id or form.deal_id.data
    )

@spv_admin_bp.route("/deals/create", methods=["GET", "POST"])
@login_required
def create_deal():
    from app.admin.spv.forms import SpvDealForm
    form = SpvDealForm()
    
    if form.validate_on_submit():
        deal = SpvDeal(
            title=form.title.data,
            slug=slugify(form.title.data),
            summary=form.summary.data,
            status=form.status.data
        )
        db.session.add(deal)
        db.session.commit()
        
        flash("Deal created successfully.", "success")
        return redirect(url_for("spv_admin_bp.spv_dashboard", deal_id=deal.id))
        
    return render_template("admin/spv/deal_form.html", form=form)

@spv_admin_bp.route("/deals/<int:deal_id>/edit", methods=["GET", "POST"])
@login_required
def edit_deal(deal_id):
    deal = SpvDeal.query.get_or_404(deal_id)
    from app.admin.spv.forms import SpvDealForm
    form = SpvDealForm(obj=deal)
    
    if form.validate_on_submit():
        deal.title = form.title.data
        deal.slug = slugify(form.title.data)
        deal.summary = form.summary.data
        deal.status = form.status.data
        
        db.session.commit()
        flash("Deal updated successfully.", "success")
        return redirect(url_for("spv_admin_bp.spv_dashboard", deal_id=deal.id))
        
    return render_template("admin/spv/deal_form.html", form=form)

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

    section_id_query = request.args.get("section_id", type=int)
    if request.method == "GET" and section_id_query:
        form.section_id.data = section_id_query

    if form.validate_on_submit():

        file_path = None

        if form.file.data:

            file = form.file.data

            import uuid
            unique_id = uuid.uuid4().hex[:8]
            filename = f"{unique_id}_{secure_filename(file.filename)}"

            upload_folder = os.path.join(
                current_app.static_folder,
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

        section_for_asset = SpvSection.query.get(form.section_id.data)

        asset = SpvAsset(
            section_id=form.section_id.data,
            title=section_for_asset.title if section_for_asset else "Asset",
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
                "spv_admin_bp.spv_dashboard",
                deal_id=asset.section.deal_id,
                section_id=asset.section_id
            )
        )

    deal_id = None
    if form.section_id.data:
        sec = SpvSection.query.get(form.section_id.data)
        if sec:
            deal_id = sec.deal_id
    elif section_id_query:
        sec = SpvSection.query.get(section_id_query)
        if sec:
            deal_id = sec.deal_id

    return render_template(
        "admin/spv/asset_form.html",
        form=form,
        deal_id=deal_id
    )

@spv_admin_bp.route(
    "/sections/<int:section_id>/edit",
    methods=["GET", "POST"]
)
@login_required
def edit_section(section_id):

    section = SpvSection.query.get_or_404(section_id)

    form = SpvSectionForm(obj=section)

    if form.validate_on_submit():

        from app.models.spv import SpvGenericSection
        title = form.title.data.title()
        generic_sec = SpvGenericSection.query.filter_by(title=title).first()
        if not generic_sec:
            db.session.add(SpvGenericSection(title=title))

        section.deal_id = form.deal_id.data

        section.title = title

        section.slug = slugify(form.title.data)

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
            url_for("spv_admin_bp.spv_dashboard", deal_id=section.deal_id, section_id=section.id)
        )

    from app.models.spv import SpvGenericSection
    generic_sections = SpvGenericSection.query.order_by(SpvGenericSection.title.asc()).all()

    return render_template(
        "admin/spv/section_form.html",
        form=form,
        section=section,
        title="Edit Section",
        generic_sections=generic_sections,
        deal_id=section.deal_id
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

        section_for_asset = SpvSection.query.get(form.section_id.data)
        if section_for_asset:
            asset.title = section_for_asset.title

        asset.asset_type = form.asset_type.data

        asset.external_url = form.external_url.data

        if form.file.data:

            file = form.file.data

            import uuid
            unique_id = uuid.uuid4().hex[:8]
            filename = f"{unique_id}_{secure_filename(file.filename)}"

            upload_dir = os.path.join(
                current_app.static_folder,
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
            url_for("spv_admin_bp.spv_dashboard", deal_id=asset.section.deal_id, section_id=asset.section_id)
        )

    return render_template(
        "admin/spv/asset_form.html",
        form=form,
        asset=asset,
        deal_id=asset.section.deal_id,
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

