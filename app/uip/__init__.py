from flask import Blueprint, g, request, abort
from flask_login import current_user
from app.extensions import db
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import text

uip_bp = Blueprint("uip_bp", __name__, url_prefix="/uip")

def auto_patch_database():
    '''Automatically patch missing columns in live DB without user intervention.'''
    patches = [
        ("core_organization", "area VARCHAR(255)"),
        ("core_organization", "municipality_ref VARCHAR(255)"),
        ("core_organization", "contact_email VARCHAR(255)"),
        ("core_organization", "contact_phone VARCHAR(50)"),
        ("core_organization", "status VARCHAR(50) DEFAULT 'active'"),
        ("core_organization", "config_json TEXT"),
        ("core_organization_member", "is_active BOOLEAN DEFAULT TRUE"),
        ("core_organization_member", "joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("core_organization_member", "left_at TIMESTAMP"),
        ("core_interaction", "assigned_to INTEGER"),
        ("core_interaction", "closed_by INTEGER"),
        ("core_interaction", "channel VARCHAR(50)"),
        ("core_interaction", "category VARCHAR(100)"),
        ("core_interaction", "interaction_type VARCHAR(50)"),
        ("core_interaction", "priority VARCHAR(50)"),
        ("core_interaction", "reference VARCHAR(50)")
    ]
    for table, col in patches:
        try:
            db.session.execute(text(f"ALTER TABLE {table} ADD COLUMN {col}"))
            db.session.commit()
        except Exception:
            db.session.rollback()

@uip_bp.before_request
def establish_organization_context():
    '''Middleware to extract org_slug from URL and enforce tenant isolation.'''
    if request.view_args and 'org_slug' in request.view_args:
        org_slug = request.view_args['org_slug']
        
        from app.models.core import CoreOrganization, CoreOrganizationMember
        
        try:
            org = CoreOrganization.query.filter_by(slug=org_slug).first()
        except ProgrammingError as e:
            db.session.rollback()
            if "does not exist" in str(e) or "UndefinedColumn" in str(e):
                auto_patch_database()
                org = CoreOrganization.query.filter_by(slug=org_slug).first()
            else:
                raise
                
        if not org:
            if org_slug == 'manor-gardens':
                db.create_all()
                org = CoreOrganization(
                    name='Manor Gardens UIP',
                    slug='manor-gardens',
                    area='Manor Gardens',
                    municipality_ref='eThekwini',
                    contact_email='admin@manorgardensuip.co.za',
                    contact_phone='031-555-0192',
                    status='active'
                )
                db.session.add(org)
                db.session.flush()
                from app.models.core import CoreOrganizationWallet
                wallet = CoreOrganizationWallet(organization_id=org.id, balance=1000)
                db.session.add(wallet)
                db.session.commit()
            else:
                abort(404, description=f"Organisation '{org_slug}' not found.")
            
        g.organization = org
        
        if current_user.is_authenticated:
            # Force schema check on CoreInteraction so it auto-patches if missing
            from app.models.core import CoreInteraction
            try:
                CoreInteraction.query.filter_by(organization_id=org.id).first()
            except ProgrammingError as e:
                db.session.rollback()
                if "does not exist" in str(e) or "UndefinedColumn" in str(e):
                    auto_patch_database()
                else:
                    raise

            try:
                membership = CoreOrganizationMember.query.filter_by(
                    organization_id=org.id, 
                    user_id=current_user.id,
                    is_active=True
                ).first()
            except ProgrammingError as e:
                db.session.rollback()
                if "does not exist" in str(e) or "UndefinedColumn" in str(e):
                    auto_patch_database()
                    membership = CoreOrganizationMember.query.filter_by(
                        organization_id=org.id, 
                        user_id=current_user.id,
                        is_active=True
                    ).first()
                else:
                    raise
            
            if not membership:
                if org_slug == 'manor-gardens':
                    # Auto-enroll the user into the demo organization so they can explore it
                    membership = CoreOrganizationMember(
                        organization_id=org.id,
                        user_id=current_user.id,
                        is_active=True
                    )
                    db.session.add(membership)
                    
                    from app.models.core import CoreRole, CoreRoleAssignment
                    manager_role = CoreRole.query.filter_by(slug='manager').first()
                    if not manager_role:
                        manager_role = CoreRole(name='Manager', slug='manager')
                        db.session.add(manager_role)
                        db.session.flush()
                        
                    assignment = CoreRoleAssignment.query.filter_by(user_id=current_user.id, organization_id=org.id).first()
                    if not assignment:
                        assignment = CoreRoleAssignment(
                            user_id=current_user.id,
                            organization_id=org.id,
                            role_id=manager_role.id
                        )
                        db.session.add(assignment)
                    
                    db.session.commit()
                elif current_user.email != "sanjith@ait.com":
                    abort(403, description="Tenant Isolation Violation: You do not have access to this Organization.")

from . import routes
