import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''@mechanic_bp.route('/mechanic/client_soa/<int:client_id>')
@login_required
def client_soa(client_id):
    from app.models.debtors import Debtor
    debtor = Debtor.query.filter_by(
        reference_id=client_id, slug_reference='mechanic', user_id=current_user.id).first()
    if not debtor:
        flash('No Statement of Account exists for this client yet.', 'info')
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
    return_url = request.args.get('return_url')
    if return_url:
        return redirect(url_for('debtors_bp.generate_soa', debtor_id=debtor.id, return_url=return_url))
    return redirect(url_for('debtors_bp.generate_soa', debtor_id=debtor.id))''',
    '''@mechanic_bp.route('/mechanic/client_soa/<int:client_id>')
@login_required
def client_soa(client_id):
    from app.models.debtors import Debtor
    debtor = Debtor.query.filter_by(
        reference_id=client_id, slug_reference='mechanic', user_id=current_user.id).first()
    if not debtor:
        flash('No Statement of Account exists for this client yet.', 'info')
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
    return redirect(url_for('mechanic_bp.client_ledger', debtor_id=debtor.id))'''
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
