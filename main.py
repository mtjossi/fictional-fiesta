import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import datetime
from time import sleep
from tqdm import tqdm
import ssl

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
today = datetime.datetime.today().date().isoformat()
ssl._create_default_https_context = ssl._create_unverified_context

def get_latest2():
    df2_filled = pd.read_excel('./codes.xlsx', sheet_name=-1)
    df2_filled['raw_number'] = 0
    df2_filled['date_raw'] = 0
    df2_filled['raw_number2'] = 0

    for i, v in tqdm(enumerate(df2_filled['ISIN']), total=df2_filled.shape[0]):
        if v not in ['UZ7011030002', 'UZ7028090007', 'UZ7035340007', 'UZ7004770002', 'UZ7021490006', 
        'UZ7016990002', 'UZ7030360000']:
            START_URL = "https://www.uzse.uz/isu_infos/STK?isu_cd="
            URL = f"{START_URL}{v}"
            temp = pd.read_html(URL)
            r = requests.get(URL, verify=False)
            sleep(1)
            soup = bs(r.text, 'lxml')
            numbers = soup.find_all(class_="trd-price")
            number = numbers[-1].text
            dates = soup.find_all(class_="text-left")
            date = pd.to_datetime(dates[-1].text.strip(), format="%d.%m.%Y").date().isoformat()

            df2_filled.loc[i, 'raw_number'] = number
            df2_filled.loc[i, 'price as of'] = date
            df2_filled.loc[i, 'date_raw'] = temp[4].iloc[0,0]
            df2_filled.loc[i, 'raw_number2'] = temp[4].iloc[0,1]

        else:
            START_URL = "http://elsissavdo.uz/results?ResultsSearch%5Btrtime%5D=&ResultsSearch%5Bstock%5D="

            URL2 = f"{START_URL}{v}"
            temp_df = pd.read_html(URL2)[0]
            temp_df = temp_df.dropna().iloc[0,[2,6]]
            if temp_df[1] != 'No results found.':
                price = temp_df[1]
                try:
                    price = float(price.replace(' ',''))
                except:
                    price = float(price)
                date = temp_df[0]
                date = pd.to_datetime(date.split(' ')[0], format="%Y-%m-%d").date().isoformat()
            else:
                price = 0
                date = 'N/A'


            df2_filled.loc[i, 'PRICE2'] = price
            df2_filled.loc[i, 'date2'] = date


    for j, k in tqdm(enumerate(df2_filled['raw_number']), total=df2_filled.shape[0]):
        if k != 0:
            price = float(k.strip().replace(',','.').replace(' ',''))
            df2_filled.loc[j, 'PRICE'] = price
            try:
                price2 = float(df2_filled.loc[j, 'raw_number2'].strip().replace(',','.').replace(' ',''))
                df2_filled.loc[j, 'PRICE2'] = price2
            except:
                df2_filled.loc[j, 'PRICE2'] = float(df2_filled.loc[j, 'raw_number2'])
            date2 = pd.to_datetime(df2_filled.loc[j, "date_raw"], format="%d.%m.%Y").date().isoformat()
            df2_filled.loc[j, 'date2'] = date2


    

    df2_filled[''] = ''
    df2_filled['Security'] = df2_filled['Stock Code']
    df2_filled['as of Date'] = df2_filled['date2']
    df2_filled['Value'] = df2_filled['PRICE2']

    df2_filled = df2_filled.drop('raw_number', axis=1)
    df2_filled = df2_filled.drop('raw_number2', axis=1)
    df2_filled = df2_filled.drop('date_raw', axis=1)
    df2_filled = df2_filled.drop('date2', axis=1)
    df2_filled = df2_filled.drop('PRICE2', axis=1)

    return df2_filled


@st.cache
def convert_df(df):
    return df.to_csv(index=None).encode('utf-8')

st.title('Testing2')

start_button = st.button("Get Data")

if start_button:
    with st.spinner("Please wait..."):
        df = get_latest2()
    st.success("Successfully got the latest data")

    for col in df.columns:
        df[col] = df[col].apply(str)

    st.dataframe(df)

    csv_file = convert_df(df)

    download_button = st.download_button(label="Download File",
    data = csv_file,
    file_name='latest.csv',
    mime='text/csv')
