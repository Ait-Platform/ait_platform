# utils/country_list.py
from __future__ import annotations
import unicodedata, difflib
from typing import Iterable, List, Dict, Tuple, Any

# Your list lives here:
# COUNTRIES = ["South Africa", ...]
# or [("South Africa","ZA"), ...]
# or [{"name":"South Africa","code":"ZA"}, ...]
try:
    COUNTRIES  # type: ignore[name-defined]
except NameError:
    COUNTRIES: List[Any] = []

def _norm(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()

# --- Lazy index building (never races import order) --------------------------
_INDEX: List[Tuple[str, str, str]] = []  # (norm_name, norm_code, original_name)
_INDEX_FINGERPRINT: Tuple[int, int] | None = None

def _build_index_if_needed() -> None:
    global _INDEX, _INDEX_FINGERPRINT
    fp = (id(COUNTRIES), len(COUNTRIES))
    if _INDEX and _INDEX_FINGERPRINT == fp:
        return
    _INDEX = []
    seen: set[Tuple[str, str]] = set()
    for nm, cd in _name_code_iter(COUNTRIES):
        key = (nm.lower(), cd.lower())
        if key in seen:
            continue
        seen.add(key)
        _INDEX.append((_norm(nm), cd.lower(), nm))
    _INDEX_FINGERPRINT = fp
# ---------------------------------------------------------------------------

def search_countries(query: str, limit: int = 20) -> List[Dict[str, str]]:
    _build_index_if_needed()

    q = _norm(query)
    if not q:
        return []

    one = len(q) == 1
    out: List[Dict[str, str]] = []

    # fast pass
    for nname, ncode, original in _INDEX:
        match = nname.startswith(q) if one else (
            nname.startswith(q) or (q in nname) or (ncode and ncode.startswith(q))
        )
        if match:
            out.append({"name": original, "code": (ncode.upper() if ncode else "")})
            if len(out) >= limit:
                return out

    # fuzzy fallback (typos) if nothing found and query has some length
    if not out and len(q) >= 3:
        norm_names = [n for (n, _, _) in _INDEX]
        for mn in difflib.get_close_matches(q, norm_names, n=limit, cutoff=0.73):
            for nname, ncode, original in _INDEX:
                if nname == mn:
                    out.append({"name": original, "code": (ncode.upper() if ncode else "")})
                    if len(out) >= limit:
                        break
            if len(out) >= limit:
                break

    return out

def resolve_country(user_input: str) -> str:
    q = (user_input or "").strip()
    if not q:
        return ""
    hits = search_countries(q, limit=1)
    return hits[0]["name"] if hits else q

def _name_code_iter(src):
    for item in src:
        name = code = ""
        if isinstance(item, str):
            name = item
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            a, b = str(item[0]).strip(), str(item[1]).strip()
            # auto-detect code-first (e.g., ("QA","Qatar")) and flip
            if len(a) == 2 and a.isalpha() and a.upper() == a and not (len(b) == 2 and b.isupper()):
                name, code = b, a
            else:
                name, code = a, b  # name-first already
        elif isinstance(item, dict):
            # supports {"name":"Qatar","code":"QA"} or similar
            name = str(item.get("name") or item.get("country") or item.get("label") or "").strip()
            code = str(item.get("code") or item.get("alpha2") or item.get("iso") or item.get("id") or "").strip()
        if name:
            yield (name, code)


COUNTRIES = [
    ("AF", "Afghanistan"),
    ("AL", "Albania"),
    ("DZ", "Algeria"),
    ("AS", "American Samoa"),
    ("AD", "Andorra"),
    ("AO", "Angola"),
    ("AI", "Anguilla"),
    ("AQ", "Antarctica"),
    ("AG", "Antigua and Barbuda"),
    ("AR", "Argentina"),
    ("AM", "Armenia"),
    ("AW", "Aruba"),
    ("AU", "Australia"),
    ("AT", "Austria"),
    ("AZ", "Azerbaijan"),
    ("BS", "Bahamas"),
    ("BH", "Bahrain"),
    ("BD", "Bangladesh"),
    ("BB", "Barbados"),
    ("BY", "Belarus"),
    ("BE", "Belgium"),
    ("BZ", "Belize"),
    ("BJ", "Benin"),
    ("BM", "Bermuda"),
    ("BT", "Bhutan"),
    ("BO", "Bolivia"),
    ("BA", "Bosnia and Herzegovina"),
    ("BW", "Botswana"),
    ("BR", "Brazil"),
    ("BN", "Brunei"),
    ("BG", "Bulgaria"),
    ("BF", "Burkina Faso"),
    ("BI", "Burundi"),
    ("KH", "Cambodia"),
    ("CM", "Cameroon"),
    ("CA", "Canada"),
    ("CV", "Cape Verde"),
    ("CF", "Central African Republic"),
    ("TD", "Chad"),
    ("CL", "Chile"),
    ("CN", "China"),
    ("CO", "Colombia"),
    ("KM", "Comoros"),
    ("CG", "Congo - Brazzaville"),
    ("CD", "Congo - Kinshasa"),
    ("CR", "Costa Rica"),
    ("HR", "Croatia"),
    ("CU", "Cuba"),
    ("CY", "Cyprus"),
    ("CZ", "Czech Republic"),
    ("DK", "Denmark"),
    ("DJ", "Djibouti"),
    ("DM", "Dominica"),
    ("DO", "Dominican Republic"),
    ("EC", "Ecuador"),
    ("EG", "Egypt"),
    ("SV", "El Salvador"),
    ("GQ", "Equatorial Guinea"),
    ("ER", "Eritrea"),
    ("EE", "Estonia"),
    ("SZ", "Eswatini"),
    ("ET", "Ethiopia"),
    ("FJ", "Fiji"),
    ("FI", "Finland"),
    ("FR", "France"),
    ("GA", "Gabon"),
    ("GM", "Gambia"),
    ("GE", "Georgia"),
    ("DE", "Germany"),
    ("GH", "Ghana"),
    ("GR", "Greece"),
    ("GD", "Grenada"),
    ("GT", "Guatemala"),
    ("GN", "Guinea"),
    ("GW", "Guinea-Bissau"),
    ("GY", "Guyana"),
    ("HT", "Haiti"),
    ("HN", "Honduras"),
    ("HU", "Hungary"),
    ("IS", "Iceland"),
    ("IN", "India"),
    ("ID", "Indonesia"),
    ("IR", "Iran"),
    ("IQ", "Iraq"),
    ("IE", "Ireland"),
    ("IL", "Israel"),
    ("IT", "Italy"),
    ("CI", "Ivory Coast"),
    ("JM", "Jamaica"),
    ("JP", "Japan"),
    ("JO", "Jordan"),
    ("KZ", "Kazakhstan"),
    ("KE", "Kenya"),
    ("KI", "Kiribati"),
    ("KW", "Kuwait"),
    ("KG", "Kyrgyzstan"),
    ("LA", "Laos"),
    ("LV", "Latvia"),
    ("LB", "Lebanon"),
    ("LS", "Lesotho"),
    ("LR", "Liberia"),
    ("LY", "Libya"),
    ("LI", "Liechtenstein"),
    ("LT", "Lithuania"),
    ("LU", "Luxembourg"),
    ("MG", "Madagascar"),
    ("MW", "Malawi"),
    ("MY", "Malaysia"),
    ("MV", "Maldives"),
    ("ML", "Mali"),
    ("MT", "Malta"),
    ("MH", "Marshall Islands"),
    ("MR", "Mauritania"),
    ("MU", "Mauritius"),
    ("MX", "Mexico"),
    ("FM", "Micronesia"),
    ("MD", "Moldova"),
    ("MC", "Monaco"),
    ("MN", "Mongolia"),
    ("ME", "Montenegro"),
    ("MA", "Morocco"),
    ("MZ", "Mozambique"),
    ("MM", "Myanmar"),
    ("NA", "Namibia"),
    ("NR", "Nauru"),
    ("NP", "Nepal"),
    ("NL", "Netherlands"),
    ("NZ", "New Zealand"),
    ("NI", "Nicaragua"),
    ("NE", "Niger"),
    ("NG", "Nigeria"),
    ("KP", "North Korea"),
    ("MK", "North Macedonia"),
    ("NO", "Norway"),
    ("OM", "Oman"),
    ("PK", "Pakistan"),
    ("PW", "Palau"),
    ("PA", "Panama"),
    ("PG", "Papua New Guinea"),
    ("PY", "Paraguay"),
    ("PE", "Peru"),
    ("PH", "Philippines"),
    ("PL", "Poland"),
    ("PT", "Portugal"),
    ("QA", "Qatar"),
    ("RO", "Romania"),
    ("RU", "Russia"),
    ("RW", "Rwanda"),
    ("KN", "Saint Kitts and Nevis"),
    ("LC", "Saint Lucia"),
    ("VC", "Saint Vincent and the Grenadines"),
    ("WS", "Samoa"),
    ("SM", "San Marino"),
    ("ST", "São Tomé and Príncipe"),
    ("SA", "Saudi Arabia"),
    ("SN", "Senegal"),
    ("RS", "Serbia"),
    ("SC", "Seychelles"),
    ("SL", "Sierra Leone"),
    ("SG", "Singapore"),
    ("SK", "Slovakia"),
    ("SI", "Slovenia"),
    ("SB", "Solomon Islands"),
    ("SO", "Somalia"),
    ("ZA", "South Africa"),
    ("KR", "South Korea"),
    ("SS", "South Sudan"),
    ("ES", "Spain"),
    ("LK", "Sri Lanka"),
    ("SD", "Sudan"),
    ("SR", "Suriname"),
    ("SE", "Sweden"),
    ("CH", "Switzerland"),
    ("SY", "Syria"),
    ("TW", "Taiwan"),
    ("TJ", "Tajikistan"),
    ("TZ", "Tanzania"),
    ("TH", "Thailand"),
    ("TL", "Timor-Leste"),
    ("TG", "Togo"),
    ("TO", "Tonga"),
    ("TT", "Trinidad and Tobago"),
    ("TN", "Tunisia"),
    ("TR", "Turkey"),
    ("TM", "Turkmenistan"),
    ("TV", "Tuvalu"),
    ("UG", "Uganda"),
    ("UA", "Ukraine"),
    ("AE", "United Arab Emirates"),
    ("GB", "United Kingdom"),
    ("US", "United States"),
    ("UY", "Uruguay"),
    ("UZ", "Uzbekistan"),
    ("VU", "Vanuatu"),
    ("VE", "Venezuela"),
    ("VN", "Vietnam"),
    ("YE", "Yemen"),
    ("ZM", "Zambia"),
    ("ZW", "Zimbabwe")
]

