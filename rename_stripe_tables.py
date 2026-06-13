from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    print("Renaming Stripe tables to Yoco in PostgreSQL...")
    
    # 1. Rename stripe_payment to yoco_payment
    try:
        db.session.execute(text("ALTER TABLE stripe_payment RENAME TO yoco_payment;"))
        print("✅ Renamed table: stripe_payment -> yoco_payment")
    except Exception as e:
        db.session.rollback()
        print("⚠️ Table stripe_payment not found or already renamed.")

    # Rename columns in yoco_payment
    try:
        db.session.execute(text("ALTER TABLE yoco_payment RENAME COLUMN stripe_session_id TO yoco_session_id;"))
        db.session.execute(text("ALTER TABLE yoco_payment RENAME COLUMN stripe_payment_intent_id TO yoco_payment_id;"))
        print("✅ Renamed columns in yoco_payment")
    except Exception as e:
        db.session.rollback()
        print("⚠️ Columns in yoco_payment not found or already renamed.")

    # 2. Rename stripe_subscription to yoco_subscription
    try:
        db.session.execute(text("ALTER TABLE stripe_subscription RENAME TO yoco_subscription;"))
        print("✅ Renamed table: stripe_subscription -> yoco_subscription")
    except Exception as e:
        db.session.rollback()
        print("⚠️ Table stripe_subscription not found or already renamed.")

    # Rename columns in yoco_subscription
    try:
        db.session.execute(text("ALTER TABLE yoco_subscription RENAME COLUMN stripe_subscription_id TO yoco_subscription_id;"))
        print("✅ Renamed columns in yoco_subscription")
    except Exception as e:
        db.session.rollback()
        print("⚠️ Columns in yoco_subscription not found or already renamed.")

    db.session.commit()
    print("Done! The database is now clean of Stripe references.")
