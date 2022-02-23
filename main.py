import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import datetime
from time import sleep
from tqdm import tqdm

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
today = datetime.datetime.today().date().isoformat()

def get_latest():
    df2_filled = pd.read_excel('./codes.xlsx', sheet_name=-1)
    df2_filled['raw_number'] = 0

    for i, v in tqdm(enumerate(df2_filled['ISIN']), total=df2_filled.shape[0]):
        if v not in ['UZ7011030002', 'UZ7028090007', 'UZ7035340007', 'UZ7004770002', 'UZ7021490006']:
            START_URL = "https://www.uzse.uz/isu_infos/STK?isu_cd="
            URL = f"{START_URL}{v}"
            r = requests.get(URL, verify=False)
            sleep(1)
            soup = bs(r.text)
            numbers = soup.find_all(class_="trd-price")
            number = numbers[-1].text
            dates = soup.find_all(class_="text-left")
            date = pd.to_datetime(dates[-1].text.strip(), format="%d.%m.%Y").date().isoformat()

            df2_filled.loc[i, 'raw_number'] = number
            df2_filled.loc[i, 'price as of'] = date

        else:
            START_URL = "http://elsissavdo.uz/results?ResultsSearch%5Btrtime%5D=&ResultsSearch%5Bstock%5D="

            URL2 = f"{START_URL}{v}"
            temp_df = pd.read_html(URL2)[0]
            temp_df = temp_df.dropna().iloc[0,[2,6]]
            if temp_df[1] != 'No results found.':
                price = temp_df[1]
                price = float(price.replace(' ',''))
                date = temp_df[0]
                date = pd.to_datetime(date.split(' ')[0], format="%Y-%m-%d").date().isoformat()
            else:
                price = 'N/A'
                date = 'N/A'


            df2_filled.loc[i, 'PRICE'] = price
            df2_filled.loc[i, 'price as of'] = date
    
    for j, k in tqdm(enumerate(df2_filled['raw_number']), total=df2_filled.shape[0]):
        if k != 0:
            price = float(k.strip().replace(',','.').replace(' ',''))
            df2_filled.loc[j, 'PRICE'] = price

    df2_filled = df2_filled.drop('raw_number', axis=1)

    df2_filled.loc[df2_filled.shape[0]+1] = ['', '','retrieved on','', today]

    return df2_filled


@st.cache
def convert_df(df):
    return df.to_csv(index=None).encode('utf-8')

st.title('Testing')

start_button = st.button("Get Data")

if start_button:
    with st.spinner("Please wait..."):
        df = get_latest()
    st.success("Successfully got the latest data")

    for col in df.columns:
        df[col] = df[col].apply(str)

    st.dataframe(df)

    csv_file = convert_df(df)

    download_button = st.download_button(label="Download File",
    data = csv_file,
    file_name='latest.csv',
    mime='text/csv')




