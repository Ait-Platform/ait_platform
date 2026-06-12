from decimal import Decimal

from flask_login import current_user

from app.models import spv
from app.models.auth import User
from app.models.spv import (
    Spv, SpvAgreement, SpvApproval, SpvBankTransaction, SpvCommitment, SpvDocumentTemplate, SpvFinancialModel, SpvFund, SpvFundInvestor, SpvInvestment, 
    SpvInvestor, SpvInvestorStatement, SpvManagementFee, SpvPayment, SpvPayout, SpvPayoutDistribution, SpvPerformanceAlert, SpvPerformanceFee, 
    SpvTransaction)
from app.extensions import db


def calculate_spv_equity(spv_id):
    investors = SpvInvestor.query.filter_by(spv_id=spv_id).all()

    total = sum(i.amount_invested for i in investors)

    if total == 0:
        return

    for investor in investors:
        investor.equity_percentage = float(investor.amount_invested / total * 100)

    db.session.commit()

def distribute_payout(spv_id, total_amount):
    spv = Spv.query.get(spv_id)
    investors = spv.investors

    if not investors:
        return

    payout = SpvPayout(
        spv_id=spv_id,
        total_amount=total_amount
    )
    db.session.add(payout)
    db.session.flush()  # get payout.id

    for investor in investors:
        share = (investor.equity_percentage or 0) / 100
        amount = total_amount * Decimal(share)

        dist = SpvPayoutDistribution(
            payout_id=payout.id,
            investor_id=investor.id,
            amount=amount
        )

        db.session.add(dist)

        # ALSO record in ledger (important)
        tx = SpvTransaction(
            spv_id=spv_id,
            investor_id=investor.id,
            amount=amount,
            transaction_type="payout"
        )
        db.session.add(tx)

    db.session.commit()

def calculate_investor_roi(investor_id):
    investor = SpvInvestor.query.get(investor_id)

    invested = investor.amount_invested or 0

    payouts = sum(p.amount for p in investor.payouts)

    if invested == 0:
        return 0

    return float((payouts / invested) * 100)

def calculate_spv_financials(spv_id):
    model = SpvFinancialModel.query.filter_by(spv_id=spv_id).first()

    if not model:
        return

    # Total cost
    model.total_project_cost = (
        (model.purchase_price or 0) +
        (model.development_cost or 0)
    )

    # Rental
    model.net_rental_income = (
        (model.gross_rental_income or 0) -
        (model.operating_expenses or 0)
    )

    rental_total = (model.net_rental_income or 0) * (model.hold_period_months or 0)

    # Exit
    sale_profit = (model.projected_sale_value or 0) - (model.total_project_cost or 0)

    # Total profit
    model.total_profit = rental_total + sale_profit

    if model.total_project_cost:
        model.roi_percentage = float(
            (model.total_profit / model.total_project_cost) * 100
        )

    db.session.commit()

def calculate_investor_projection(investor):
    model = investor.spv.financial_model

    if not model or not investor.equity_percentage:
        return 0

    share = investor.equity_percentage / 100

    return float(model.total_profit * share)

def get_spv_funding_status(spv_id):
    spv = Spv.query.get(spv_id)

    committed = sum(c.amount for c in spv.commitments if c.status == "confirmed")
    invested = sum(i.amount_invested for i in spv.investors)

    return {
        "target": float(spv.target_raise or 0),
        "committed": float(committed),
        "invested": float(invested),
        "remaining": float((spv.target_raise or 0) - invested)
    }

def convert_commitment_to_investment(commitment_id):
    commitment = SpvCommitment.query.get(commitment_id)

    if not commitment or commitment.status != "confirmed":
        return

    investment = SpvInvestment(
        spv_id=commitment.spv_id,
        user_id=commitment.user_id,
        commitment_id=commitment.id,
        total_amount=commitment.amount,
        status="pending_payment"
    )

    db.session.add(investment)

    commitment.status = "converted"

    db.session.commit()

    return investment

def record_payment(investment_id, amount):
    investment = SpvInvestment.query.get(investment_id)

    payment = SpvPayment(
        investment_id=investment.id,
        amount=amount
    )

    db.session.add(payment)

    total_paid = sum(p.amount for p in investment.payments) + amount

    if total_paid >= investment.total_amount:
        investment.status = "active"

        # push into investor + ledger
        investor = SpvInvestor.query.filter_by(
            spv_id=investment.spv_id,
            user_id=investment.user_id
        ).first()

        if not investor:
            investor = SpvInvestor(
                spv_id=investment.spv_id,
                user_id=investment.user_id
            )
            db.session.add(investor)

        investor.amount_invested += total_paid

        tx = SpvTransaction(
            spv_id=investment.spv_id,
            investor=investor,
            amount=total_paid,
            transaction_type="investment"
        )

        db.session.add(tx)

        calculate_spv_equity(investment.spv_id)

    db.session.commit()

def allocate_fund_capital(fund_id):
    fund = SpvFund.query.get(fund_id)

    total_fund_capital = sum(i.amount_invested for i in fund.investors)

    for link in fund.fund_spvs:
        spv = link.spv

        allocation = (link.allocation_percentage or 0) / 100
        amount_to_spv = total_fund_capital * allocation

        # push into SPV as investment
        tx = SpvTransaction(
            spv_id=spv.id,
            amount=amount_to_spv,
            transaction_type="fund_allocation"
        )

        db.session.add(tx)

    db.session.commit()

def calculate_fund_returns(fund_id):
    fund = SpvFund.query.get(fund_id)

    total_returns = 0

    for link in fund.fund_spvs:
        spv = link.spv

        payouts = sum(p.total_amount for p in spv.payouts)

        allocation = (link.allocation_percentage or 0) / 100

        total_returns += payouts * allocation

    return float(total_returns)

def has_role(user, role_name):
    return any(r.role == role_name for r in user.roles)

def request_payout(spv_id, amount, user_id):
    approval = SpvApproval(
        entity_type="payout",
        entity_id=spv_id,
        status="pending",
        requested_by=user_id
    )

    db.session.add(approval)
    db.session.commit()

def approve_payout(approval_id, approver_id):
    approval = SpvApproval.query.get(approval_id)

    if not approval or approval.status != "pending":
        return

    approval.status = "approved"
    approval.approved_by = approver_id

    db.session.commit()

    # NOW execute
    distribute_payout(approval.entity_id, approval.amount)

def can_invest(spv):
    return spv.lifecycle_stage == "fundraising"

def get_platform_metrics():
    total_spv_capital = sum(s.target_raise or 0 for s in Spv.query.all())
    total_invested = sum(i.amount_invested for i in SpvInvestor.query.all())

    total_fund_capital = sum(i.amount_invested for i in SpvFundInvestor.query.all())

    total_investors = len(set(i.user_id for i in SpvInvestor.query.all()))

    return {
        "spv_capital": float(total_spv_capital),
        "invested": float(total_invested),
        "fund_capital": float(total_fund_capital),
        "investors": total_investors
    }

def generate_investor_statements(period):
    investors = SpvInvestor.query.all()

    for investor in investors:
        payouts = sum(p.amount for p in investor.payouts)
        invested = investor.amount_invested or 0

        roi = float((payouts / invested) * 100) if invested else 0

        statement = SpvInvestorStatement(
            user_id=investor.user_id,
            spv_id=investor.spv_id,
            period=period,
            total_invested=invested,
            total_returns=payouts,
            roi=roi
        )

        db.session.add(statement)

    db.session.commit()

def check_spv_performance(spv_id):
    spv = Spv.query.get(spv_id)
    model = spv.financial_model

    if not model:
        return

    # Example rule: ROI dropped below expectation
    if model.roi_percentage and model.roi_percentage < 10:
        alert = SpvPerformanceAlert(
            spv_id=spv_id,
            alert_type="underperformance",
            message="Projected ROI dropped below 10%"
        )
        db.session.add(alert)

    db.session.commit()

def get_capital_deployment(spv_id):
    spv = Spv.query.get(spv_id)

    invested = sum(i.amount_invested for i in spv.investors)

    deployed = sum(
        t.amount for t in spv.transactions
        if t.transaction_type in ["expense", "development"]
    )

    return {
        "invested": float(invested),
        "deployed": float(deployed),
        "remaining": float(invested - deployed)
    }

def suggest_fund_allocation(fund_id):
    fund = SpvFund.query.get(fund_id)

    suggestions = []

    for link in fund.fund_spvs:
        spv = link.spv
        model = spv.financial_model

        if model and model.roi_percentage:
            suggestions.append({
                "spv": spv.name,
                "roi": model.roi_percentage
            })

    # sort best to worst
    return sorted(suggestions, key=lambda x: x["roi"], reverse=True)

def notify_user(user_id, message):
    print(f"Notify {user_id}: {message}")  # replace with email later

def reconcile_payment(payment_id, bank_tx_id):
    payment = SpvPayment.query.get(payment_id)
    bank_tx = SpvBankTransaction.query.get(bank_tx_id)

    if not payment or not bank_tx or bank_tx.matched:
        return

    if payment.amount != bank_tx.amount:
        return  # strict match only

    bank_tx.matched = True

    payment.reference = bank_tx.reference

    db.session.commit()

def generate_agreement(investment_id):
    investment = SpvInvestment.query.get(investment_id)
    template = SpvDocumentTemplate.query.first()

    content = template.content.format(
        investor_name=investment.user.full_name,
        amount=investment.total_amount,
        spv_name=investment.spv.name
    )

    # save file path (PDF later)
    agreement = SpvAgreement(
        investment_id=investment.id,
        document_path=f"/agreements/{investment.id}.txt"
    )

    db.session.add(agreement)
    db.session.commit()

    return content

def send_email(to, subject, body):
    # plug SMTP / SendGrid later
    send_email(
    User.email,
    "Investment Confirmed",
    f"Your investment in {Spv.name} is now active."
)

    print(f"EMAIL → {to}: {subject}")

def funding_progress(spv):
    invested = sum(i.amount_invested for i in spv.investors)
    target = spv.target_raise or 0

    if not target:
        return 0

    return float((invested / target) * 100)

def get_current_tenant():
    return current_user.tenant_id

def calculate_fees(spv_id):
    spv = Spv.query.get(spv_id)

    profit = spv.financial_model.total_profit or 0

    performance_fee = SpvPerformanceFee.query.filter_by(spv_id=spv_id).first()
    management_fee = SpvManagementFee.query.filter_by(spv_id=spv_id).first()

    perf_amount = profit * (performance_fee.percentage / 100) if performance_fee else 0

    mgmt_amount = 0
    if management_fee:
        invested = sum(i.amount_invested for i in spv.investors)
        mgmt_amount = invested * (management_fee.percentage / 100)

    return {
        "performance_fee": float(perf_amount),
        "management_fee": float(mgmt_amount)
    }




