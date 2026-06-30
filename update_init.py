import re

with open('templates/program_billing/ai_onboarding.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''document.addEventListener('alpine:init', () => {
  Alpine.data('onboardingWizard', () => ({
    totalBills: parseInt(new URLSearchParams(window.location.search).get('bills') || 1),
    statements: parseInt(new URLSearchParams(window.location.search).get('tenants') || 1),
    isBulk: new URLSearchParams(window.location.search).get('is_bulk') || 'no',
    subMetersExpected: parseInt(new URLSearchParams(window.location.search).get('sub_meters') || 0),
    totalProperties: parseInt(new URLSearchParams(window.location.search).get('total_properties') || 1),
    currentProperty: parseInt(new URLSearchParams(window.location.search).get('current_property') || 1),
    
    currentBillIndex: 1,
    view: 'upload','''

injection = '''document.addEventListener('alpine:init', () => {
  Alpine.data('onboardingWizard', () => ({
    totalBills: {% if draft_property %}{{ draft_property.expected_bills }}{% else %}parseInt(new URLSearchParams(window.location.search).get('bills') || 1){% endif %},
    statements: {% if draft_property %}{{ draft_property.expected_tenants }}{% else %}parseInt(new URLSearchParams(window.location.search).get('tenants') || 1){% endif %},
    isBulk: {% if draft_property %}'{{ 'yes' if draft_property.is_bulk_metered else 'no' }}'{% else %}new URLSearchParams(window.location.search).get('is_bulk') || 'no'{% endif %},
    subMetersExpected: {% if draft_property %}{{ draft_property.expected_sub_meters }}{% else %}parseInt(new URLSearchParams(window.location.search).get('sub_meters') || 0){% endif %},
    totalProperties: parseInt(new URLSearchParams(window.location.search).get('total_properties') || 1),
    currentProperty: {% if draft_property %}{{ draft_property.id }}{% else %}parseInt(new URLSearchParams(window.location.search).get('current_property') || 1){% endif %},
    
    currentBillIndex: 1,
    view: new URLSearchParams(window.location.search).get('view') || 'upload','''

content = content.replace(target, injection)

with open('templates/program_billing/ai_onboarding.html', 'w', encoding='utf-8') as f:
    f.write(content)
