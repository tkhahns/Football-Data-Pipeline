import requests
import pandas as pd
from bs4 import BeautifulSoup
import re  # Added to handle comments in HTML

def generate_squadlist(url):
    res = requests.get(url)
    # The next two lines get around the issue with comments breaking the parsing.
    comm = re.compile("<!--|-->")
    soup = BeautifulSoup(comm.sub("", res.text), 'lxml')
    all_tables = soup.findAll("tbody")
    # print(f'tables: {all_tables}')
    table = all_tables[0]  # Assuming the first table is the one we need
    cols = []
    for header in table.find_all('th'):
        cols.append(header.get('data-stat'))
    print(f'Columns: ', cols)

    columns = cols[8:41]  # Adjusted to match the number of columns in the data
    players = cols[41:-2]

    rows = []  # Initialize list to store all rows of data
    for rownum, row in enumerate(table.find_all('tr')):  # Find all rows in table
        if len(row.find_all('td')) > 0:
            rowdata = []  # Initialize list of row data
            for i in range(len(row.find_all('td'))):  # Get all column values for row
                rowdata.append(row.find_all('td')[i].text)
            rows.append(rowdata)

    df = pd.DataFrame(rows, columns=columns)

    # Drop rows to match the length of players
    df = df.iloc[:len(players)]

    df["Player"] = players
    df['Nation'] = df['Nation'].str[3:]
    df.set_index("Player")

    return df