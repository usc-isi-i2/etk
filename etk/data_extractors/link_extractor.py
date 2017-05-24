from lxml.html import fromstring
from urlparse import urljoin
from urlparse import urlparse


def extract_links(html, url):
    doc = fromstring(html)
    base_url = get_base_url(url)
    try:
        doc.make_links_absolute(base_url, resolve_base_href=True)
    except:
        pass

    results = list()
    for link_tuple in doc.iterlinks():
        link = link_tuple[0]
        o = dict()
        o['metadata'] = dict()
        if link.tag == 'a':
            results.append(create_result_object(link, 'href', 'href'))
        elif link.tag == 'img':
            results.append(create_result_object(link, 'src', 'image'))
        elif link.tag == 'video':
            results.append(create_result_object(link, 'src', 'video'))
    return results


def create_result_object(element, attrib_key, type):
    o = dict()
    o['metadata'] = dict()
    o['value'] = element.attrib[attrib_key]
    o['metadata']['type'] = type
    if element.text:
        o['metadata']['text'] = element.text
    return o


def get_base_url(url):
    o = urlparse(url)
    return o.scheme + '://' + o.netloc


def make_urls_absolute(url_list, base_url):
    urls = set()
    for link in url_list:
        if not link.startswith('http'):
            urls.add(urljoin(base_url, link))
        else:
            urls.add(link)
    return list(urls)
