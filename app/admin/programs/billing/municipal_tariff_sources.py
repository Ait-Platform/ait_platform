"""Official-source discovery and PDF retrieval only; no rates or database writes."""
from datetime import datetime, timezone
from hashlib import sha256
from html.parser import HTMLParser
from io import BytesIO
import re
import ssl
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

from pypdf import PdfReader

HOSTS = {'www.durban.gov.za', 'durban.gov.za', 'planning.durban.gov.za', 'electricity.durban.gov.za'}
MAX_PDF = 25 * 1024 * 1024
MAX_HTML = 2 * 1024 * 1024
MAX_DOWNLOADS = 3

SECTIGO_INTERMEDIATE = (
    "-----BEGIN CERTIFICATE-----\n"
    "MIIGTDCCBDSgAwIBAgIQOXpmzCdWNi4NqofKbqvjsTANBgkqhkiG9w0BAQwFADBf\n"
    "MQswCQYDVQQGEwJHQjEYMBYGA1UEChMPU2VjdGlnbyBMaW1pdGVkMTYwNAYDVQQD\n"
    "Ey1TZWN0aWdvIFB1YmxpYyBTZXJ2ZXIgQXV0aGVudGljYXRpb24gUm9vdCBSNDYw\n"
    "HhcNMjEwMzIyMDAwMDAwWhcNMzYwMzIxMjM1OTU5WjBgMQswCQYDVQQGEwJHQjEY\n"
    "MBYGA1UEChMPU2VjdGlnbyBMaW1pdGVkMTcwNQYDVQQDEy5TZWN0aWdvIFB1Ymxp\n"
    "YyBTZXJ2ZXIgQXV0aGVudGljYXRpb24gQ0EgRFYgUjM2MIIBojANBgkqhkiG9w0B\n"
    "AQEFAAOCAY8AMIIBigKCAYEAljZf2HIz7+SPUPQCQObZYcrxLTHYdf1ZtMRe7Yeq\n"
    "RPSwygz16qJ9cAWtWNTcuICc++p8Dct7zNGxCpqmEtqifO7NvuB5dEVexXn9RFFH\n"
    "12Hm+NtPRQgXIFjx6MSJcNWuVO3XGE57L1mHlcQYj+g4hny90aFh2SCZCDEVkAja\n"
    "EMMfYPKuCjHuuF+bzHFb/9gV8P9+ekcHENF2nR1efGWSKwnfG5RawlkaQDpRtZTm\n"
    "M64TIsv/r7cyFO4nSjs1jLdXYdz5q3a4L0NoabZfbdxVb+CUEHfB0bpulZQtH1Rv\n"
    "38e/lIdP7OTTIlZh6OYL6NhxP8So0/sht/4J9mqIGxRFc0/pC8suja+wcIUna0HB\n"
    "pXKfXTKpzgis+zmXDL06ASJf5E4A2/m+Hp6b84sfPAwQ766rI65mh50S0Di9E3Pn\n"
    "2WcaJc+PILsBmYpgtmgWTR9eV9otfKRUBfzHUHcVgarub/XluEpRlTtZudU5xbFN\n"
    "xx/DgMrXLUAPaI60fZ6wA+PTAgMBAAGjggGBMIIBfTAfBgNVHSMEGDAWgBRWc1hk\n"
    "lfmSGrASKgRieaFAFYghSTAdBgNVHQ4EFgQUaMASFhgOr872h6YyV6NGUV3LBycw\n"
    "DgYDVR0PAQH/BAQDAgGGMBIGA1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0lBBYwFAYI\n"
    "KwYBBQUHAwEGCCsGAQUFBwMCMBsGA1UdIAQUMBIwBgYEVR0gADAIBgZngQwBAgEw\n"
    "VAYDVR0fBE0wSzBJoEegRYZDaHR0cDovL2NybC5zZWN0aWdvLmNvbS9TZWN0aWdv\n"
    "UHVibGljU2VydmVyQXV0aGVudGljYXRpb25Sb290UjQ2LmNybDCBhAYIKwYBBQUH\n"
    "AQEEeDB2ME8GCCsGAQUFBzAChkNodHRwOi8vY3J0LnNlY3RpZ28uY29tL1NlY3Rp\n"
    "Z29QdWJsaWNTZXJ2ZXJBdXRoZW50aWNhdGlvblJvb3RSNDYucDdjMCMGCCsGAQUF\n"
    "BzABhhdodHRwOi8vb2NzcC5zZWN0aWdvLmNvbTANBgkqhkiG9w0BAQwFAAOCAgEA\n"
    "YtOC9Fy+TqECFw40IospI92kLGgoSZGPOSQXMBqmsGWZUQ7rux7cj1du6d9rD6C8\n"
    "ze1B2eQjkrGkIL/OF1s7vSmgYVafsRoZd/IHUrkoQvX8FZwUsmPu7amgBfaY3g+d\n"
    "q1x0jNGKb6I6Bzdl6LgMD9qxp+3i7GQOnd9J8LFSietY6Z4jUBzVoOoz8iAU84OF\n"
    "h2HhAuiPw1ai0VnY38RTI+8kepGWVfGxfBWzwH9uIjeooIeaosVFvE8cmYUB4TSH\n"
    "5dUyD0jHct2+8ceKEtIoFU/FfHq/mDaVnvcDCZXtIgitdMFQdMZaVehmObyhRdDD\n"
    "4NQCs0gaI9AAgFj4L9QtkARzhQLNyRf87Kln+YU0lgCGr9HLg3rGO8q+Y4ppLsOd\n"
    "unQZ6ZxPNGIfOApbPVf5hCe58EZwiWdHIMn9lPP6+F404y8NNugbQixBber+x536\n"
    "WrZhFZLjEkhp7fFXf9r32rNPfb74X/U90Bdy4lzp3+X1ukh1BuMxA/EEhDoTOS3l\n"
    "7ABvc7BYSQubQ2490OcdkIzUh3ZwDrakMVrbaTxUM2p24N6dB+ns2zptWCva6jzW\n"
    "r8IWKIMxzxLPv5Kt3ePKcUdvkBU/smqujSczTzzSjIoR5QqQA6lN1ZRSnuHIWCvh\n"
    "JEltkYnTAH41QJ6SAWO66GrrUESwN/cgZzL4JLEqz1Y=\n"
    "-----END CERTIFICATE-----\n"
)


class RetrievalError(ValueError):
    pass


def year_start(year):
    if not re.fullmatch(r'20\d{2}/\d{2}', str(year)):
        raise RetrievalError('Select a financial year in YYYY/YY format.')
    first = int(year[:4])
    if int(year[-2:]) != (first + 1) % 100 or not 2024 <= first <= 2098:
        raise RetrievalError('The financial year must run from 1 July to the following 30 June.')
    return first


def official_url(url, *, pdf=False):
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        raise RetrievalError('Invalid official source address.') from None
    if (parts.scheme != 'https' or parts.hostname not in HOSTS or port not in (None,443)
            or parts.username or parts.password or '\\' in url or any(ord(c)<32 for c in url)):
        raise RetrievalError('The source must be a supported official eThekwini HTTPS address.')
    if pdf and (not parts.path.startswith('/uploads/') or not parts.path.lower().endswith('.pdf')):
        raise RetrievalError('Only official uploaded PDF publications may be retrieved.')
    return urlunsplit((parts.scheme,parts.netloc,parts.path,parts.query,''))


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def read_url(url, limit, deadline):
    """TLS verification, bounded reads and separately validated redirects."""
    try:
        import certifi
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    except Exception:
        ssl_ctx = ssl.create_default_context()
    try:
        ssl_ctx.load_verify_locations(cadata=SECTIGO_INTERMEDIATE)
    except Exception:
        pass
    opener = build_opener(HTTPSHandler(context=ssl_ctx),NoRedirect())
    for _ in range(4):
        url = official_url(url,pdf=limit==MAX_PDF)
        if time.monotonic() >= deadline:
            raise RetrievalError('Municipal retrieval time limit reached; existing tariffs are unchanged.')
        try:
            remaining = deadline - time.monotonic()
            req_timeout = 15 if remaining > 15 else max(5, remaining)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 AIT-Municipal-Tariff-Review/1.0',
                'Accept': 'text/html,application/xhtml+xml,application/pdf,*/*;q=0.8',
            }
            response = opener.open(Request(url,headers=headers),timeout=req_timeout)
        except HTTPError as exc:
            if exc.code in (301,302,303,307,308) and exc.headers.get('Location'):
                url = official_url(urljoin(url,exc.headers['Location']),pdf=limit==MAX_PDF)
                exc.close();continue
            code=exc.code;exc.close()
            raise RetrievalError('Official municipal source returned HTTP '+str(code)+'.') from None
        except (URLError,TimeoutError,OSError):
            raise RetrievalError('Official municipal source could not be reached securely (network, TLS or timeout).') from None
        with response:
            headers={key.lower():value for key,value in response.headers.items()}
            try:
                size=int(headers.get('content-length','0'))
            except ValueError:
                raise RetrievalError('Official source supplied an invalid document size.') from None
            if size<0 or size>limit:
                raise RetrievalError('Official document exceeds the supported download size.')
            chunks=[];received=0
            while True:
                if time.monotonic()>=deadline:
                    raise RetrievalError('Municipal retrieval time limit reached.')
                try:chunk=response.read(min(65536,limit-received+1))
                except (OSError,TimeoutError):raise RetrievalError('Municipal download was interrupted; no incomplete document was accepted.') from None
                if not chunk:break
                received+=len(chunk)
                if received>limit:raise RetrievalError('Official document exceeds the supported download size.')
                chunks.append(chunk)
            return b''.join(chunks),headers,url
    raise RetrievalError('Official source redirected too many times.')


def years_in(text):
    result=set()
    for first,last in re.findall(r'(?<!\d)(20\d{2})[ _/\-]*(20\d{2}|\d{2})(?!\d)',text):
        if int(last) in (int(first)+1,(int(first)+1)%100):
            result.add(first+'/'+str(int(first)+1)[-2:])
    return sorted(result)


class Links(HTMLParser):
    def __init__(self):
        super().__init__();self.links=[];self.current=None
    def handle_starttag(self, tag, attrs):
        if tag=='a':self.current={'href':dict(attrs).get('href',''),'text':[]}
    def handle_data(self, data):
        if self.current is not None:self.current['text'].append(data)
    def handle_endtag(self, tag):
        if tag=='a' and self.current is not None:
            self.current['label']=' '.join(' '.join(self.current.pop('text')).split())
            self.links.append(self.current);self.current=None


def publication_date(label):
    match=re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s+(20\d{2})\b',label)
    if match:
        months=('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')
        try:return datetime(int(match[3]),months.index(match[1])+1,int(match[2])).date().isoformat()
        except ValueError:pass
    return None


def discovery(year, fetch=read_url, deadline=None):
    first=year_start(year);deadline=deadline or time.monotonic()+75
    indexes=[
        'https://www.durban.gov.za/pages/government/documents?'+urlencode({'d':'Tariffs'}),
        'https://electricity.durban.gov.za/pages/government/documents?'+urlencode({'d':'Tariffs'}),
        'https://www.durban.gov.za/pages/government/documents?'+urlencode({'d':f'Tariffs {first} - {first+1}'}),
        'https://electricity.durban.gov.za/pages/government/documents?'+urlencode({'d':f'{first}_{first+1}'}),
        'https://www.durban.gov.za/pages/government/documents?'+urlencode({'d':'Service Tariffs'})]
    found={};issues=[]
    for index in indexes:
        try:
            content,headers,final=fetch(index,MAX_HTML,deadline)
            if 'html' not in headers.get('content-type','').lower():
                raise RetrievalError('Official document index did not return HTML.')
            markup=content.decode('utf-8',errors='replace')
            if 'cf-chl-' in markup or '<title>Just a moment' in markup:
                raise RetrievalError('Official site requires a browser challenge; automatic discovery cannot proceed.')
            parser=Links();parser.feed(markup)
            for link in parser.links:
                candidate=urljoin(final,link['href'])
                label=link['label'][:1000]
                if 'tariff' not in (candidate+' '+label).lower():continue
                try:candidate=official_url(candidate,pdf=True)
                except (RetrievalError,ValueError):continue
                declared=years_in(urlsplit(candidate).path+' '+label)
                if year not in declared:continue
                words=(urlsplit(candidate).path.rsplit('/',1)[-1]+' '+label).lower()
                kind=('DRAFT' if re.search(r'\bdraft\b',words) else
                      'PROPOSED' if re.search(r'\bproposed\b',words) else
                      'FINAL_LABELLED' if re.search(r'\bfinal\b',words) else 'UNCLASSIFIED')
                key=urlsplit(candidate).path
                found.setdefault(key,dict(source_url=candidate,index_url=final,label=label,
                    financial_year=year,declared_years=declared,publication_status=kind,
                    publication_date=publication_date(label),publication_date_basis='Municipal listing date, not adoption date' if publication_date(label) else 'Not stated'))
        except RetrievalError as exc:
            issues.append(index+': '+str(exc))
    ordered=sorted(found.values(),key=lambda item:(item['publication_status']=='FINAL_LABELLED',item['publication_date'] or '',item['source_url']),reverse=True)
    if not ordered:issues.append('No matching tariff PDFs were found on the checked official indexes. No rates were invented or changed.')
    return ordered,issues


def retrieve(candidate, fetch=read_url, deadline=None):
    deadline=deadline or time.monotonic()+75
    data,headers,final=fetch(candidate['source_url'],MAX_PDF,deadline)
    if not 0<len(data)<=MAX_PDF:raise RetrievalError('Municipal PDF exceeds the supported size or is empty.')
    if not data.startswith(b'%PDF-'):
        raise RetrievalError('Municipal publication is not a PDF (it may be an HTML error page).')
    try:
        reader=PdfReader(BytesIO(data))
        if reader.is_encrypted or not 0<len(reader.pages)<=2000:raise ValueError()
        text=' '.join(page.extract_text() or '' for page in list(reader.pages)[:2])[:20000]
    except Exception:
        raise RetrievalError('Municipal PDF is damaged, encrypted or unsupported; it was not accepted.') from None
    declared=years_in(text)
    issues=['Adoption/approval is not verified by retrieval alone; verification and activation require separate checks.']
    final_years=years_in(urlsplit(final).path)
    if final_years and candidate['financial_year'] not in final_years:
        issues.append('Redirected PDF filename year conflicts with the municipal listing.')
    if len(candidate['declared_years'])!=1:issues.append('The source listing refers to multiple financial years; scope needs review.')
    if not declared:issues.append('The financial year is indicated by the municipal link/listing; PDF title text did not independently confirm it.')
    elif candidate['financial_year'] not in declared:issues.append('PDF title year conflicts with the requested financial year.')
    metadata=dict(candidate,source_url=official_url(final,pdf=True),sha256=sha256(data).hexdigest(),
                  size_bytes=len(data),page_count=len(reader.pages),document_years=declared,
                  retrieved_at=datetime.now(timezone.utc).isoformat(),http_last_modified=headers.get('last-modified'),approval_status='UNVERIFIED',issues=issues)
    return data,metadata
