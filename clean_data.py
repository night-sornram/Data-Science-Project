import pandas as pd
from keybert import KeyBERT
model = KeyBERT('distilbert-base-nli-mean-tokens')

column_names = ['title', 'abstract', 'classifications', 'date_publication', 'affiliation', 'references', 'keywords']

df = pd.read_csv('./scopus_messages.csv', header=0, names=column_names, on_bad_lines='skip', usecols=['title', 'abstract', 'date_publication', 'keywords'])

df.drop_duplicates(inplace=True)
df.dropna(inplace=True)

def convert_date(date_str):
    data = date_str.replace("{'year': '", "").replace("', 'month': '", ",").replace("', 'day': '", ",").replace("'}", "")
    data = data.split(',')
    
    month_map = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
        'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    }
    try:
        year = data[0]
        month = month_map[data[1]]
        day = f"{int(data[2]):02d}"
        return f"{year}-{month}-{day}"
    except:
        return None

def find_keywords(df):
    keywords = df["keywords"]
    if len(keywords) == 0:
        keywords = []
        key = model.extract_keywords(df["abstract"])
        for kw , sc in key:
            keywords.append(kw)
        df["keywords"] = keywords

df["title"] = df["title"].apply(lambda x: x.replace("Title:", ""))
df["date_publication"] = df["date_publication"].apply(convert_date)
df.to_csv('scopus_messages_cleaned.csv', index=False, header=True)