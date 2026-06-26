import os

files = {
    'templates/program_adv_math/about.html': [
        ("url_for('adv_math_bp.payflow')", "url_for('public_bp.coming_soon', subject_slug='adv_math')")
    ],
    'templates/program_spv/about.html': [
        ("url_for('spv_bp.price_spv')", "url_for('public_bp.coming_soon', subject_slug='spv')")
    ],
    'templates/program_hds/about.html': [
        ("<form action=\"{{ url_for('hds_bp.start_trial') }}\" method=\"POST\">", ""),
        ("<input type=\"hidden\" name=\"csrf_token\" value=\"{{ csrf_token() }}\">", ""),
        ("<button type=\"submit\"", "<a href=\"{{ url_for('public_bp.coming_soon', subject_slug='hds') }}\""),
        ("</button>", "</a>"),
        ("</form>", "")
    ]
}

for fp, replacements in files.items():
    if os.path.exists(fp):
        with open(fp, 'r', encoding='utf-8') as f:
            content = f.read()
        for old, new in replacements:
            content = content.replace(old, new)
        with open(fp, 'w', encoding='utf-8') as f:
            f.write(content)
