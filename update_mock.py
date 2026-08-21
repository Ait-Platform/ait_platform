import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Update document_preview mock_job
mock_original = '''    mock_job = {
        'id': 0,
        'job_number': 'PREVIEW-001',
        'created_at': datetime.utcnow(),
        'invoices': [{'status': 'Unpaid'}],
        'vehicle': {
            'client': {
                'name': 'John Doe',
                'phone': '082 555 1234',
                'email': 'john@example.com'
            },
            'registration_number': 'CA 123 456',
            'vin': 'XYZ1234567890ABC',
            'make': 'Toyota',
            'model': 'Hilux',
            'year': 2018
        },
        'status': 'Completed',
        'labor_lines': [
            {'description': 'Replace brake pads', 'hours': 2.0, 'rate': 650.0, 'total': 1300.0}
        ],
        'part_lines': [
            {'description': 'Front Brake Pads (Set)', 'quantity': 1, 'unit_price': 850.0, 'total': 850.0}
        ]
    }'''

mock_new = '''    mock_job = {
        'id': 0,
        'job_number': 'PREVIEW-SAMPLE',
        'created_at': datetime.utcnow(),
        'invoices': [{'status': 'Unpaid'}],
        'vehicle': {
            'client': {
                'name': 'SAMPLE CUSTOMER',
                'phone': '082 000 0000',
                'email': 'sample@example.com'
            },
            'registration_number': 'SAMPLE-REG',
            'vin': 'SAMPLEVIN12345678',
            'make': 'SAMPLE',
            'model': 'VEHICLE',
            'year': 2026,
            'engine_no': 'SAMPLE-ENG',
            'disk_license_no': 'SAMPLE-LIC',
            'gvm': '2000',
            'tare': '1500'
        },
        'status': 'Completed',
        'labor_lines': [
            {'description': 'Sample Labor Task', 'hours': 2.0, 'rate': 650.0, 'total': 1300.0}
        ],
        'part_lines': [
            {'description': 'Sample Part', 'quantity': 1, 'unit_price': 850.0, 'total': 850.0}
        ]
    }'''

content = content.replace(mock_original, mock_new)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated mock_job for preview")
