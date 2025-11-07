# app/services/stripe_client.py
import stripe, os
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")
BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:8000")
