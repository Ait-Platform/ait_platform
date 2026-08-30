from flask import g, current_app
from app.models.core import CoreOrganizationWallet, CoreOrganizationLedger, CoreAiRequest, CoreAiUsage
from app.extensions import db
import os

class LunaGateway:
    """
    The strict boundary enforcing tenant isolation, permissions, and token balances.
    """
    
    @staticmethod
    def ask_luna(prompt, interaction_id=None):
        org = getattr(g, 'organization', None)
        if not org:
            raise Exception("AI Gateway Violation: No organization context established.")
            
        wallet = CoreOrganizationWallet.query.filter_by(organization_id=org.id).first()
        if not wallet:
            # Auto-provision a wallet if it doesn't exist (1000 starter tokens per Rollout Spec)
            wallet = CoreOrganizationWallet(organization_id=org.id, balance=1000)
            db.session.add(wallet)
            db.session.commit()
            
        if wallet.balance <= 0:
            return {
                "status": "suspended",
                "message": "AI services suspended due to zero token balance. Core CRM remains unaffected."
            }
            
        # In a real environment, we'd call the AI API here.
        # For now, we simulate the Luna response.
        simulated_response = f"Simulated Luna response for: {prompt[:30]}..."
        tokens_used = 15 # Simulated token usage
        cost = tokens_used * 1 # 1 cent per token
        
        # Deduct from wallet
        wallet.balance -= cost
        
        # Write to ledger
        ledger = CoreOrganizationLedger(
            wallet_id=wallet.id,
            amount=-cost,
            description=f"Luna API Request (Interaction: {interaction_id})"
        )
        db.session.add(ledger)
        
        # Write audit logs
        request_log = CoreAiRequest(
            organization_id=org.id,
            interaction_id=interaction_id,
            model_requested="luna",
            prompt_text=prompt,
            response_text=simulated_response,
            status="completed"
        )
        db.session.add(request_log)
        db.session.commit()
        
        # Add usage stats
        usage = CoreAiUsage(
            request_id=request_log.id,
            tokens_in=10,
            tokens_out=5,
            cost_cents=cost
        )
        db.session.add(usage)
        db.session.commit()
        
        return {
            "status": "success",
            "message": simulated_response,
            "cost_cents": cost,
            "remaining_balance": wallet.balance
        }
