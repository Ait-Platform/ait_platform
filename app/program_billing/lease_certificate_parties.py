"""Read-only Certificate party blocks from saved lease particulars and selected identities."""
from copy import deepcopy
from .lease_certificate import present, selected_tenants, authority_status

NATURAL = {'individual', 'natural_person'}
ENTITIES = {'company', 'trust', 'juristic_person', 'npc'}
REP_FIELDS = ('signatory_name', 'signatory_identity_number', 'signatory_capacity', 'representative_phone', 'representative_email')


def text(value):
    return str(value).strip() if present(value) else ''


def first_text(party, *keys):
    return next((text(party.get(key)) for key in keys if present(party.get(key))), '')


def representative(party):
    return {name: text(party.get(key)) for name, key in zip(('name', 'identity_number', 'capacity', 'phone', 'email'), REP_FIELDS)}


def has_representative(rep):
    return any(rep.values())


def signs_personally(party, rep):
    if party['type'] not in NATURAL or not has_representative(rep):
        return False
    identity, rep_identity = party['identity_number'], rep['identity_number']
    if identity and rep_identity:
        return identity.casefold() == rep_identity.casefold()
    return False  # A similar name alone cannot establish that an agent is the party.


def blocks(items, owners=(), tenants=()):
    """Never merge parties or infer their type from a representative field."""
    saved_lessors = items.get('_contracting_lessors', {}).get('identities', [])
    if not saved_lessors:
        saved_lessors = [deepcopy(owner) for owner in owners]
    saved_lessees = selected_tenants(items)
    groups = []
    global_lessor = items.get('_parties', {}).get('lessor', {})
    for title, label, saved, register, identifier, register_id in (
        ('LESSORS', 'Lessor', saved_lessors, owners, 'owner_id', 'owner_id'),
        ('LESSEES', 'Lessee', saved_lessees, tenants, 'tenant_id', 'id')):
        entries = []
        for source_index, source in enumerate(saved):
            if not isinstance(source, dict):
                continue
            # Only an explicitly selected register identity may fill missing snapshot fields.
            matches = [row for row in register if source.get(identifier) is not None and str(row.get(register_id)) == str(source[identifier])]
            merged = deepcopy(matches[0]) if len(matches) == 1 else {}
            merged.update({key: value for key, value in source.items() if present(value)})
            name = first_text(source, 'legal_name', 'name') or first_text(merged, 'legal_name', 'name')
            identity = text(merged.get('identity_number'))
            if not (name or identity or source.get(identifier) is not None):
                continue  # Empty capture slots are not contracting parties.
            kind = first_text(source, 'party_type', 'type', 'identity_type', 'entity_type') or first_text(merged, 'party_type', 'type', 'identity_type', 'entity_type')
            party = {'name': name, 'identity_number': identity, 'type': kind,
                     'representative': representative(merged), 'warnings': [], 'source_index': source_index}
            if not name or not identity:
                party['warnings'].append('Contracting party identity particulars are incomplete.')
            if kind not in NATURAL | ENTITIES:
                party['warnings'].append('Party type is not recorded; representative requirements need review.')
            entries.append(party)
        group_rep = representative(global_lessor) if label == 'Lessor' else {}
        # A group-wide legacy mandate is unambiguous only when there is one Lessor.
        if len(entries) == 1 and has_representative(group_rep) and not has_representative(entries[0]['representative']):
            entries[0]['representative'] = group_rep
            group_rep = {}
        for party in entries:
            rep = party['representative']
            if signs_personally(party, rep):
                party['representative'] = rep = {}
            required = party['type'] in ENTITIES
            if required and not has_representative(rep):
                party['warnings'].append('Required representative details are missing or not linked to this party.')
            elif has_representative(rep):
                if not rep.get('name') or not rep.get('identity_number'):
                    party['warnings'].append('Recorded representative identity particulars are incomplete.')
                key = 'landlords' if label == 'Lessor' else 'tenant-' + str(party['source_index'])
                raw = {'key': key, 'name': 'All contracting landlords' if label == 'Lessor' else party['name'],
                       'representative': rep.get('name', ''), 'identity_number': rep.get('identity_number', ''), 'capacity': rep.get('capacity', '')}
                authority = authority_status(items, key, raw, saved, True)
                if authority.get('confirmed') != 'Confirmed':
                    party['warnings'].append('Representative signing authority is not confirmed in the saved particulars.')
        groups.append({'title': title, 'label': label, 'parties': entries,
                       'group_representative': group_rep if has_representative(group_rep) else {}})
    return groups
