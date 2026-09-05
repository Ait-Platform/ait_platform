from app import create_app
from app.extensions import db
from app.models.auth import User
from app.models.core import (
    CoreOrganization, CoreRole, CoreRoleAssignment, 
    CoreInteraction, CoreTask
)
import random

app = create_app()
with app.app_context():
    mg = CoreOrganization.query.filter_by(slug="manor_gardens").first()
    
    # 1. Create or fetch Dummy Users
    res_user = User.query.filter_by(email="resident@manorgardens.com").first()
    if not res_user:
        res_user = User(
            email="resident@manorgardens.com", 
            first_name="Jane", 
            last_name="Resident",
            is_active=True
        )
        res_user.set_password("Password123!")
        db.session.add(res_user)
        
    rec_user = User.query.filter_by(email="reception@manorgardens.com").first()
    if not rec_user:
        rec_user = User(
            email="reception@manorgardens.com", 
            first_name="Tom", 
            last_name="Reception",
            is_active=True
        )
        rec_user.set_password("Password123!")
        db.session.add(rec_user)
        
    db.session.flush()

    # 2. Assign Roles
    res_role = CoreRole.query.filter_by(slug="resident", organization_id=mg.id).first()
    rec_role = CoreRole.query.filter_by(slug="receptionist", organization_id=mg.id).first()
    
    if not CoreRoleAssignment.query.filter_by(user_id=res_user.id, organization_id=mg.id).first():
        db.session.add(CoreRoleAssignment(user_id=res_user.id, organization_id=mg.id, role_id=res_role.id))
        
    if not CoreRoleAssignment.query.filter_by(user_id=rec_user.id, organization_id=mg.id).first():
        db.session.add(CoreRoleAssignment(user_id=rec_user.id, organization_id=mg.id, role_id=rec_role.id))
        
    db.session.flush()

    # 3. Simulate an Interaction (Broken Streetlight)
    ref = f"MG-{random.randint(10000, 99999)}"
    interaction = CoreInteraction(
        reference=ref,
        organization_id=mg.id,
        creator_id=res_user.id,
        interaction_type="service issue",
        title="Broken Streetlight on Oak Avenue",
        description="The streetlight outside number 42 has been flickering and is now completely dead.",
        status="open",
        priority="high"
    )
    db.session.add(interaction)
    db.session.flush()
    
    # 4. Generate a Task for the Receptionist
    task = CoreTask(
        interaction_id=interaction.id,
        assignee_id=rec_user.id,
        title="Log municipal fault",
        description="Call the municipality and log a fault for the broken streetlight on Oak Avenue.",
        status="pending"
    )
    db.session.add(task)
    
    db.session.commit()
    
    print("=== PHASE 10 PILOT TEST SUCCESS ===")
    print(f"Created Resident: {res_user.email}")
    print(f"Created Receptionist: {rec_user.email}")
    print(f"Interaction Generated: {interaction.reference} - {interaction.title}")
    print(f"Task Generated: {task.title} (Assigned to {task.assignee_id})")
