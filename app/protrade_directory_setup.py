"""Narrow main-directory setup and signed new-shop events; no report operations."""
from flask import abort, current_app, jsonify, render_template, request, url_for
from flask_login import current_user
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from app.models.mechanic import MechShop
from app.protrade_output import folder_label


def signer():
    return URLSafeTimedSerializer(current_app.secret_key, salt='protrade-new-shop-directories-v1')


def registration_token(shop):
    return signer().dumps(dict(user=current_user.id, shop=shop.id))


def registered_program_name():
    from app.models.auth import AuthSubject
    program = AuthSubject.query.filter_by(slug='mechanic').first()
    if not program or not program.name:
        abort(503, description='The registered program name is unavailable. No directories were created.')
    return folder_label(program.name, 'Program')


def context_data():
    shop = None
    token = request.args.get('registration', '')
    if token:
        try:
            claim = signer().loads(token, max_age=1800)
        except (BadSignature, SignatureExpired):
            abort(400, description='The new-shop directory request expired or is invalid. No existing shops will be processed.')
        if claim.get('user') != current_user.id:
            abort(403)
        shop = MechShop.query.filter_by(id=claim.get('shop'), user_id=current_user.id, onboarding_status='active').first()
        if not shop:
            abort(404)
    return dict(user=str(current_user.id), programKey='protrade', program=registered_program_name(),
                consultant=folder_label(current_user.name, 'User-'+str(current_user.id)),
                shop=dict(id=str(shop.id), name=folder_label(shop.business_name, 'Shop-'+str(shop.id))) if shop else None)


def setup_page():
    context = context_data()
    response = current_app.make_response(render_template('program_mechanic/main_directory.html',
        directory_setup=dict(scope=dict(user=context['user'], property='protrade'),
            url=url_for('mechanic_bp.protrade_directory_context', registration=request.args.get('registration', ''))),
        directory_metadata=context))
    response.headers['Cache-Control'] = 'private, no-store'
    return response


def setup_context():
    response = jsonify(context_data())
    response.headers['Cache-Control'] = 'private, no-store'
    return response
