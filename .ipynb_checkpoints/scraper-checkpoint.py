# This is a template for a Python scraper on morph.io (https://morph.io)
# including some code snippets below that you should find helpful

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import os
from datetime import datetime
import fuzzymatcher

# +
mainLink = 'https://www.ufc.com'

rankingsLink = mainLink + '/rankings'
# -

data = requests.get(rankingsLink)
soup = BeautifulSoup(data.text, 'html.parser')


segments = soup.find_all('div', {'class': 'view-grouping-content'}, href=False)   

excludeList = ["Women's Featherweight"]
#excludeList = []
dfList = []
for segment in segments:
    weightclass = segment.find('div', {'class': 'info'}).find('h4').text.strip()
    if weightclass in excludeList:
        print(f"skipping {weightclass}")
        continue
    
    subDf = pd.DataFrame()
    print(f"{weightclass} Top 15")
    fighters = segment.find_all('td', {'class':'views-field views-field-title'})
    fightersLink = [mainLink + fighter.find('a', href=True)['href'] for fighter in fighters]
    fightersImg = []
    fightersNickName = []
    fightersRecord = []
    fightersWins = []
    fightersLoses = []
    fightersDraws = []
    for fighterpage in fightersLink:
        fighterData = requests.get(fighterpage)
        fighterSoup = BeautifulSoup(fighterData.text, 'html.parser')
        src = fighterSoup.find('div', {'class':'hero-profile__image-wrap'}).find('img')['src']
        nickName = fighterSoup.find('p', {'class':'hero-profile__nickname'})
        if nickName is None:
            nickName = 'N/A'
        else:
            nickName = nickName.text.strip()
        record = fighterSoup.find('p', {'class':'hero-profile__division-body'}).text.strip().split(' ')[0]
        fightersRecord.append(record)
        record = record.split('-')
        if len(record) == 3:
            wins = record[0]
            loss = record[1]
            draw = record[2]
        else:
            wins = record[0]
            loss = record[1]
            draw = '0'
            
        fightersImg.append(src)
        fightersNickName.append(nickName)
        fightersWins.append(wins)
        fightersLoses.append(loss)
        fightersDraws.append(draw)
        
    fighters = [fighter.text.strip() for fighter in fighters]
    print(fighters)
    print("==========\n")
    subDf['fighters'] = fighters
    subDf['nickName'] = fightersNickName
    subDf['record'] = fightersRecord
    subDf['wins'] = fightersWins
    subDf['losses'] = fightersLoses
    subDf['draws'] = fightersDraws
    subDf['weightclass'] = weightclass
    subDf['rank'] = [r for r in range(1, len(fighters)+1)]
    subDf['img'] = fightersImg
    subDf = subDf[['weightclass','rank', 'fighters','nickName','record','wins', 'losses', 'draws', 'img']]
    dfList.append(subDf)


finalDf = pd.concat(dfList)
finalDf = finalDf.reset_index().rename(columns={'index':'_id'})


# +
snapshotDate = datetime.now().date()
finalDf['date'] = snapshotDate

finalDf['date'] = pd.to_datetime(finalDf['date'])
# -

conn = sqlite3.connect('data.sqlite')
matched_df.to_sql('data', conn, if_exists='append')
print(f'UFC Fighters Rankings snapshot for {snapshotDate} successfully scraped and updated')
conn.close()
