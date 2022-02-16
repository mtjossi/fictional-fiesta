import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import datetime
from time import sleep
from tqdm import tqdm

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
today = datetime.datetime.today().date().isoformat()

@st.cache
def get_latest():
    df_filled = pd.read_excel('./codes.xlsx', sheet_name=-1)
    df_filled = df_filled.iloc[1:,:5]
    df_filled.reset_index(inplace=True, drop=True)

    START_URL = "https://www.uzse.uz/isu_infos/STK?isu_cd="

    df_filled['raw_number'] = 0
    df_filled['retrieved on'] = 0

    for i, v in tqdm(enumerate(df_filled['ISIN']), total=df_filled.shape[0]):
        if v != '0000':
            URL = f"{START_URL}{v}"
            r = requests.get(URL, verify=False)
            sleep(1)
            soup = bs(r.text, features='lxml')
            numbers = soup.find_all(class_="trd-price")
            number = numbers[-1].text
            dates = soup.find_all(class_="text-left")
            date = pd.to_datetime(dates[-1].text.strip(), format="%d.%m.%Y").date().isoformat()

        df_filled.loc[i, 'raw_number'] = number
        df_filled.loc[i, 'retrieved on'] = today
        df_filled.loc[i, 'price as of'] = date


    for i, v in enumerate(df_filled['raw_number']):
        if v != 0:

            price = float(v.strip().replace(',','.').replace(' ',''))
            df_filled.loc[i, 'PRICE'] = price

    df_filled = df_filled.drop('raw_number', axis=1)

    START_URL = "http://elsissavdo.uz/results?ResultsSearch%5Btrtime%5D=&ResultsSearch%5Bstock%5D="

    df_filled['PRICE 2'] = 0
    df_filled['price as of 2'] = 0

    for i, v in enumerate(df_filled['ISIN']):
        if v != '0000':
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


        df_filled.loc[i, 'PRICE 2'] = price
        df_filled.loc[i, 'price as of 2'] = date

    return df_filled

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




