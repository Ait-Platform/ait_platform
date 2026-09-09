"""Exact reported CSV and option diagnostics through the real import endpoint."""
import pytest
from bootstrap import uip
from test_completion import csv_post,preview_token,csv_file

HEADER="reference,name,member_type,email,phone,is_active,eligibility_status\n"
ROW="MOCK001,Thabo Mkhize,person,thabo.mkhize@example.invalid,0825550101,true,eligible\n"

@pytest.mark.parametrize("bom",[False,True])
def test_exact_reported_csv_preview_and_commit(client,bom):
    content=(HEADER+ROW).encode("utf-8-sig" if bom else "utf-8")
    preview=csv_post(client,"members",content,operation="preview")
    assert preview.status_code==200,preview.get_data(as_text=True)
    assert uip.UipMemberProfile.query.count()==0
    committed=csv_post(client,"members",content,operation="commit",preview_token=preview_token(preview))
    assert committed.status_code==302,committed.get_data(as_text=True)
    member=uip.UipMemberProfile.query.one()
    assert (member.reference,member.member_type,member.is_active,member.eligibility_status)==("MOCK001","person",True,"eligible")


@pytest.mark.parametrize("column,value,allowed",[
    ("member_type","OWNER",("person","business")),
    ("is_active","TRUE",("true","false")),
    ("eligibility_status","ELIGIBLE",("unverified","eligible","ineligible")),
    ("member_type","person\u200b",("person","business")),
    ("member_type","<script>alert(1)</script>",("person","business")),
])
def test_csv_option_error_names_actual_column_value_and_rolls_back(client,column,value,allowed):
    import html
    from bootstrap import core
    valid=dict(zip(HEADER.strip().split(","),ROW.strip().split(",")))
    invalid={**valid,"reference":"MOCK002","email":"second@example.invalid",column:value}
    before=core.CoreOrganizationMember.query.count()
    response=csv_post(client,"members",csv_file("members",[valid,invalid]),operation="preview")
    assert response.status_code==400
    body=response.get_data(as_text=True)
    plain=html.unescape(body)
    assert f"CSV row 3: column '{column}' has invalid value {ascii(value)}" in plain
    assert all(ascii(option) in plain for option in allowed)
    assert "Nothing imported." in plain
    assert "<script>alert(1)</script>" not in body
    assert uip.UipMemberProfile.query.count()==0
    assert core.CoreOrganizationMember.query.count()==before


def test_all_documented_member_options_are_accepted(client):
    import itertools
    rows=[]
    for i,(member_type,active,eligibility) in enumerate(itertools.product(("person","business"),("true","false"),("unverified","eligible","ineligible")),1):
        rows.append(dict(reference=f"OPTIONS{i}",name=f"Option test {i}",member_type=member_type,email=f"option{i}@example.invalid",phone="",is_active=active,eligibility_status=eligibility))
    content=csv_file("members",rows)
    preview=csv_post(client,"members",content,operation="preview")
    assert preview.status_code==200,preview.get_data(as_text=True)
    assert csv_post(client,"members",content,operation="commit",preview_token=preview_token(preview)).status_code==302
    assert uip.UipMemberProfile.query.count()==12
