(() => {
    const data = JSON.parse(document.getElementById('intake-register-data').textContent);
    const memberSelect = document.getElementById('member_id');
    const propertySelect = document.getElementById('property_id');
    const memberSearch = document.getElementById('member-search');
    const propertySearch = document.getElementById('property-search');
    const normal = value => (value || '').normalize('NFKC').toLocaleLowerCase().trim();
    const memberLabel = m => `${m.name} / ${m.reference}`;
    const propertyLabel = p => `${p.address} / ${p.reference} / Rates: ${p.rates_reference || 'Missing'}`;
    const currentProperties = memberId => data.properties.filter(p => data.relationships.some(
        r => String(r.member_id) === memberId && r.property_id === p.id));
    const option = (label, value) => new Option(label, String(value));

    function filterMembers() {
        const selected = memberSelect.value;
        const query = normal(memberSearch.value);
        const matches = data.members.filter(m => {
            const properties = currentProperties(String(m.id));
            const fields = [m.name, m.reference, m.email, m.phone,
                ...properties.flatMap(p => [p.address, p.reference, p.rates_reference])];
            return fields.some(value => normal(value).includes(query)) ||
                (/^[+\d\s().-]+$/.test(query) && query.replace(/\D/g, '').length > 0 &&
                 (m.phone || '').replace(/\D/g, '').includes(query.replace(/\D/g, '')));
        });
        memberSelect.replaceChildren(option('No registered ratepayer / general enquiry', ''));
        // Keep the committed selection visible even while searching for a replacement.
        data.members.filter(m => matches.includes(m) || String(m.id) === selected)
            .forEach(m => memberSelect.add(option(memberLabel(m), m.id)));
        memberSelect.value = selected;
        document.getElementById('member-results').textContent = `${matches.length} matching ratepayer(s). Choose a result below.`;
    }

    function renderProperties(selected) {
        const linked = currentProperties(memberSelect.value);
        const query = normal(propertySearch.value);
        propertySelect.replaceChildren(option(linked.length > 1 ? 'Choose a current linked property' : 'No property linked', ''));
        const groups = [['Current linked properties', linked],
            ['Other properties (no current relationship to selected ratepayer)', data.properties.filter(p => !linked.includes(p))]];
        groups.forEach(([label, properties]) => {
            const group = document.createElement('optgroup');
            group.label = label;
            properties.filter(p => String(p.id) === selected || normal(propertyLabel(p)).includes(query))
                .forEach(p => group.append(option(propertyLabel(p), p.id)));
            if (group.children.length) propertySelect.append(group);
        });
        propertySelect.value = selected;
    }

    function summary() {
        const member = data.members.find(m => String(m.id) === memberSelect.value);
        const property = data.properties.find(p => String(p.id) === propertySelect.value);
        const relationships = data.relationships.filter(r => String(r.member_id) === memberSelect.value && String(r.property_id) === propertySelect.value);
        document.getElementById('member-selection').textContent = member ? `Selected ratepayer: ${memberLabel(member)}` : 'No registered ratepayer selected.';
        const fields = [['Ratepayer', member?.name], ['Member reference', member?.reference],
            ['Phone', member ? member.phone || 'Missing from register' : null],
            ['Email', member ? member.email || 'Missing from register' : null],
            ['Property reference', property?.reference], ['Address', property?.address],
            ['Rates reference', property ? property.rates_reference || 'Missing from register' : null],
            ['Current relationship', relationships.length ? relationships.map(r => `${r.relationship} (${r.verified ? 'verified' : 'unverified'})`).join(', ') : 'No current relationship recorded']];
        const container = document.getElementById('intake-register-summary');
        container.replaceChildren();
        fields.forEach(([label, value]) => {
            const group = document.createElement('div');
            const term = document.createElement('dt');
            const detail = document.createElement('dd');
            term.textContent = label;
            detail.textContent = value || 'Not selected';
            group.append(term, detail);
            container.append(group);
        });
        [['add-ratepayer', 'property_id', propertySelect.value], ['add-property', 'member_id', memberSelect.value]].forEach(([id, key, value]) => {
            const a = document.getElementById(id);
            if (a) {
                const url = new URL(a.getAttribute('href'), location.origin === 'null' ? 'http://localhost' : location.origin);
                if (value) url.searchParams.set(key, value); else url.searchParams.delete(key);
                a.setAttribute('href', url.pathname + url.search);
            }
        });
    }

    function selectMember(preserveProperty = false) {
        const member = data.members.find(m => String(m.id) === memberSelect.value);
        memberSearch.value = member ? memberLabel(member) : '';
        propertySearch.value = '';
        const linked = currentProperties(memberSelect.value);
        const selected = preserveProperty && propertySelect.value ? propertySelect.value : linked.length === 1 ? String(linked[0].id) : '';
        renderProperties(selected);
        summary();
    }
    memberSearch.addEventListener('input', filterMembers);
    memberSelect.addEventListener('change', () => selectMember());
    propertySearch.addEventListener('input', () => renderProperties(propertySelect.value));
    propertySelect.addEventListener('change', summary);
    document.getElementById('general-enquiry').addEventListener('click', () => {
        memberSelect.value = '';
        selectMember();
        filterMembers();
    });
    selectMember(true);
})();
