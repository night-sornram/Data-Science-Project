import pandas as pd

column_names = ['title', 'abstract', 'classifications', 'date_publication', 'affiliation', 'references', 'keywords']

df = pd.read_csv('./scopus_messages.csv', header=None, names=column_names, on_bad_lines='skip')

df.drop_duplicates(inplace=True)

def convert_date(date_str):
    data = date_str.replace("{'year': '", "").replace("', 'month': '", ",").replace("', 'day': '", ",").replace("'}", "")
    data = data.split(',')
    
    month_map = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
        'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    }
    if len(data) != 3:
        return "date_plublication"
    year = data[0]
    month = month_map[data[1]]
    day = f"{int(data[2]):02d}"
    return f"{year}-{month}-{day}"

df["title"] = df["title"].apply(lambda x: x.replace("Title:", ""))

df["date_publication"] = df["date_publication"].apply(convert_date)


df.to_csv('scopus_messages_cleaned.csv', index=False, header=False)

