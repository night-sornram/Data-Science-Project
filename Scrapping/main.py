import time
import re
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import avro.schema
import avro.io
import io
import json
import os
from tqdm import tqdm

from keybert import KeyBERT

# Initialize the KeyBERT model
model = KeyBERT('distilbert-base-nli-mean-tokens')


schema_path = '../scopus.avsc'
with open(schema_path, 'r') as schema_file:
    schema_string = schema_file.read()
schema = avro.schema.parse(schema_string)

topic = 'scopus'
kafka_broker = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=kafka_broker)

def serialize(schema, obj):
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer = avro.io.DatumWriter(schema)
    writer.write(obj, encoder)
    return bytes_writer.getvalue()

def is_valid_url(url):
    # Check if the URL starts with 'https://arxiv.org/'
    return url.startswith('https://export.arxiv.org/')


arxiv_url = 'https://export.arxiv.org/'

data = requests.get(arxiv_url)
soup = BeautifulSoup(data.text, 'html.parser')
# Get the link from all subject
links = []

links = [a['href'] for a in soup.find_all('a', href=True)]
links = [ l for l in links if '/recent' in l]
links = [f'https://export.arxiv.org{l}' for l in links]

# let's try to get the 100 papers
papers = []
papers_title_set = set()
target = 1000
break_flag = False
# for loop to get the 100 papers
classifications = []


for link in links:
    data = requests.get(link)
    print(f'Getting {link}')
    soup = BeautifulSoup(data.text, 'html.parser')
    links_paper = [a['href'] for a in soup.find_all('a', href=True)]
    # all paper must have abs in link but some of them have html, if it not have html i will skip it in status code from requests
    links_abs = [ f'https://export.arxiv.org{l}' for l in links_paper if '/abs/' in l]
    links_html = [ l.replace('abs','html') for l in links_abs ]
    # for loop to get the 10 papers from each link
    for i in range(len(links_abs)):
        link_html = links_html[i]
        link_abs = links_abs[i]
        print(f'scrapping {links_abs[i]} and {links_html[i]} in {link}')
        # Validate the URLs
        if not is_valid_url(link_html) or not is_valid_url(link_abs):
            print(f"Skipping invalid URL: {link_html} or {link_abs}")
            continue
        data2 = requests.get(link_abs)
        soup2 = BeautifulSoup(data2.text, 'html.parser')
        paper = requests.get(link_html)
        # if there is no html it mean their is no references and affliation
        if paper.status_code != 200:
            print('No html', paper.status_code)
            continue
        soup = BeautifulSoup(paper.text, 'html.parser')
        title = soup.select('h1.ltx_title')
        if title:
            title = soup.select('h1.ltx_title')[0].text
        elif soup2.select('h1.title'):
            title = soup2.select('h1.title')[0].text
        else:
            print('No Title of html is not working')
            continue
        title = title.strip().replace('\n','')

        # this check if the title is already exist if it exist i will skip it
        if title in papers_title_set:
            print('Title already exist')
            continue
        papers_title_set.add(title)

        abstract = soup.select('div.ltx_abstract p')
        if abstract:
            abstract = soup.select('div.ltx_abstract p')[0].text.strip().replace('\n','')
        elif soup2.select('blockquote.abstract'):
            abstract = soup2.select('blockquote.abstract')[0].text.strip().replace('\n','')
        else:
            print('No abstract')
            continue
        pub_date = soup.select('div.ltx_dates')
        pub_date_locate = soup2.find('div', {'class': 'submission-history'})
        pub_date = pub_date_locate.find('b').find_next_sibling(string=True).strip().strip('"')
        date_match = re.search(r'(\d{1,2}) (\w+) (\d{4})', pub_date)
        if date_match:
            pub_day, pub_month, pub_year = date_match.groups()
            date_publication = {
                'day': pub_day,
                'month': pub_month,
                'year': pub_year
            }
        elif pub_date:
            pub = pub_date[0].text.strip().replace('(','').replace(')','')
            if ';' in pub:
                print('No date')
                continue
            pub_year = pub.split(',')[-1].strip()
            pub_month = pub.split(' ')[0]
            pub_day = pub.split(' ')[1]

            date_publication = {
                'year': pub_year,
                'month': pub_month,
                'day': pub_day
            }
        else:
            date_publication = {
                'day': 'None',
                'month': 'None',
                'year': 'None'
            }
        affiliations = soup.select('span.ltx_role_affiliation')
        if not affiliations:
            affiliations = soup.select('span.ltx_role_address')
        affliliatioin_papers = []
        if affiliations:
            affiliations = [a.text.replace('\n','') for a in affiliations ]
            for a in affiliations:
                affiliation_full = a
                affiliation = a.split(',')
                if len(affiliation) < 2:
                    continue
                affiliation_country = affiliation[-1].replace('\n','').strip()
                affiliation_city = affiliation[-2].strip().replace('\n','')
                affilname = affiliation_full.strip().replace('\n','')
                affiliation_object = {
                    'affilname': affilname,
                    'affiliation_country': affiliation_country,
                    'affiliation_city': affiliation_city
                }
                affliliatioin_papers.append(affiliation_object)
        else:
            affliliatioin_papers = []
        references = soup.select('span.ltx_bibblock')
        if references:
            references = [r.text.replace('\xa0', ' ').strip() for r in references]
        else:
            references = []
        keywords = soup.select('div.ltx_keywords')
        if keywords:
            keywords = soup.select('div.ltx_keywords')[0]
            keywords = keywords.text.replace('\n','').strip().split(',')
        elif abstract:
            keywords = [] 
            # Example text
            text = abstract
            # Extract keywords
            key = model.extract_keywords(text)
            for kw , sc in key:
                keywords.append(kw)
        else:
            keywords = [] 
        paper = {
            "title": title,
            "abstract": abstract,
            "classifications": classifications,
            "date_publication": {
                "year": date_publication['year'],
                "month": date_publication['month'],
                "day": date_publication['day']
            },
            "affiliation": [{
                "affiliation-city": affiliation['affiliation_city'] if 'affiliation_city' in affiliation else 'None',
                "@id": affiliation['affilation_id'] if 'affilation_id' in affiliation else 'None',
                "affilname": affiliation['affilname'] if 'affilname' in affiliation else 'None',
                "@href": affiliation['affiliation_href'] if 'affiliation_href' in affiliation else 'None',
                "affiliation-country": affiliation['affiliation_country'] if 'affiliation_country' in affiliation else 'None',
            } for affiliation in affliliatioin_papers],
            "references": references,
            "keywords": keywords,
        }
        serialized_data = serialize(schema, paper)
        producer.send(topic, serialized_data)
        producer.flush()
        papers.append(paper)
        print(f'Getting paper {len(papers)} from {link} from {link_html} and {link_abs} with title {title}')
        print(f'link index: {links.index(link)} and link_abs index: {links_abs.index(link_abs)}')
        print('------------------------------------------')
        if len(papers) == target:
            break_flag = True
            break
        # time.sleep(2)
    if break_flag:
        break


