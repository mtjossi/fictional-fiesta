import io
import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import datetime
from time import sleep
from tqdm import tqdm
import ssl

ssl._create_default_https_context = ssl._create_unverified_context
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
today = datetime.datetime.today().date().isoformat()

def get_latest2():
    df2_filled = pd.read_excel('./codes.xlsx', sheet_name=-1)
    df2_filled['raw_number'] = ''
    df2_filled['date_raw'] = ''
    df2_filled['raw_number2'] = ''
    df2_filled['PRICE'] = None
    df2_filled['PRICE2'] = None
    df2_filled['date2'] = None

    errors = []

    for i, v in tqdm(enumerate(df2_filled['ISIN']), total=df2_filled.shape[0]):
        print(f"Now trying {v}")
        try:
            if v not in ['UZ7011030002', 'UZ7028090007', 'UZ7035340007', 'UZ7004770002', 'UZ7021490006',
            'UZ7016990002', 'UZ7030360000', 'UZ7032740001', 'UZ7016550004', 'UZ7016530006','UZ7026620003',
            'UZ7017850007', 'UZ7047650005', 'UZ7004510002']:

                START_URL = "https://www.uzse.uz/isu_infos/STK?isu_cd="
                URL = f"{START_URL}{v}"
                r = requests.get(URL, verify=False, timeout=15)
                sleep(1)
                soup = bs(r.text, 'lxml')
                numbers = soup.find_all(class_="trd-price")
                if not numbers:
                    raise ValueError(f"No trd-price element found")
                number = numbers[-1].text
                dates = soup.find_all(class_="text-left")
                if not dates:
                    raise ValueError(f"No text-left element found")
                date = pd.to_datetime(dates[-1].text.strip(), format="%d.%m.%Y").date().isoformat()
                tables = soup.find_all("table", attrs={"class":"table centered-table table-bordered"})
                if len(tables) < 4:
                    raise ValueError(f"Expected at least 4 tables, found {len(tables)}")
                table_body = tables[3].find("tbody")
                rows = table_body.find_all("tr") if table_body else []
                if not rows:
                    raise ValueError("No rows in table body")
                table_row = rows[0]
                cells = table_row.find_all("td")
                if len(cells) < 2:
                    raise ValueError("Not enough cells in table row")
                table_date = cells[0]
                table_price = cells[1]

                df2_filled.loc[i, 'raw_number'] = number
                df2_filled.loc[i, 'price as of'] = date
                df2_filled.loc[i, 'date_raw'] = table_date.text.strip()
                df2_filled.loc[i, 'raw_number2'] = table_price.text.strip()

            else:
                URL2 = f"https://uzse.uz/otcmarkets/trade_results?start_date=&end_date=&search_key={v}"
                try:
                    # temp_df = pd.read_html(requests.get(URL2, verify=False, timeout=15).text)[0]
                    response = requests.get(URL2, verify=False)
                    response.raise_for_status() # Raise an exception for HTTP errors
                    temp_df = pd.read_html(io.StringIO(response.text))[0]
                    temp_df = temp_df.dropna().iloc[0,[2,6]]
                    if temp_df.iloc[1] != 'No results found.':
                        price = temp_df.iloc[1]
                        try:
                            price = float(price.replace(' ',''))
                        except:
                            price = float(price)
                        date = temp_df.iloc[0]
                        date = pd.to_datetime(date.split(' ')[0], format="%Y-%m-%d").date().isoformat()
                    else:
                        price = 0
                        date = 'N/A'
                except Exception as e:
                    raise ValueError(f"OTC fetch failed: {e}")

                df2_filled.loc[i, 'PRICE2'] = price
                df2_filled.loc[i, 'date2'] = date
                df2_filled.loc[i, 'PRICE'] = price
                df2_filled.loc[i, 'price as of'] = date

        except Exception as e:
            print(f"ERROR for {v}: {e}")
            errors.append((v, str(e)))
            continue


    for j, k in tqdm(enumerate(df2_filled['raw_number']), total=df2_filled.shape[0]):
        if k:
            print(k)
            try:
                price = float(k.strip().replace(',','').replace(' ',''))
                df2_filled.loc[j, 'PRICE'] = price
                try:
                    price2 = float(df2_filled.loc[j, 'raw_number2'].strip().replace(',','').replace(' ',''))
                    df2_filled.loc[j, 'PRICE2'] = price2
                except:
                    df2_filled.loc[j, 'PRICE2'] = float(df2_filled.loc[j, 'raw_number2'])
                date2 = pd.to_datetime(df2_filled.loc[j, "date_raw"], format="%d.%m.%Y").date().isoformat()
                df2_filled.loc[j, 'date2'] = date2
            except Exception as e:
                isin = df2_filled.loc[j, 'ISIN']
                print(f"ERROR processing row {j} ({isin}): {e}")
                errors.append((isin, f"Post-processing failed: {e}"))


    df2_filled[''] = ''
    df2_filled['Security'] = df2_filled['Stock Code']
    df2_filled['as of Date'] = df2_filled['date2']
    df2_filled['as of Date'] = df2_filled['as of Date'].apply(
        lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%m/%d/%Y") if isinstance(d, str) and d != 'N/A' and d != '0' else d
    )
    df2_filled['Value'] = df2_filled['PRICE2']

    df2_filled = df2_filled.drop('raw_number', axis=1)
    df2_filled = df2_filled.drop('raw_number2', axis=1)
    df2_filled = df2_filled.drop('date_raw', axis=1)
    df2_filled = df2_filled.drop('date2', axis=1)
    df2_filled = df2_filled.drop('PRICE2', axis=1)

    return df2_filled, errors

def get_latest3():
    df2_filled = pd.read_excel('./codes.xlsx', sheet_name=-1)
    df2_filled['raw_number'] = ''
    df2_filled['date_raw'] = ''
    df2_filled['raw_number2'] = ''
    df2_filled['PRICE'] = None
    df2_filled['PRICE2'] = None
    df2_filled['date2'] = None

    errors = []

    for i, v in tqdm(enumerate(df2_filled['ISIN']), total=df2_filled.shape[0]):
        try:
            if v not in ['UZ7011030002', 'UZ7028090007', 'UZ7035340007', 'UZ7004770002', 'UZ7021490006',
            'UZ7016990002', 'UZ7030360000', 'UZ7032740001', 'UZ7016550004', 'UZ7016530006','UZ7026620003',
            'UZ7017850007', 'UZ7047650005', 'UZ7004510002']:

                START_URL = "https://www.uzse.uz/isu_infos/STK?isu_cd="
                print(v)
                URL = f"{START_URL}{v}"
                r = requests.get(URL, verify=False, timeout=15)
                sleep(1)
                soup = bs(r.text, 'lxml')
                numbers = soup.find_all(class_="trd-price")
                if not numbers:
                    raise ValueError("No trd-price element found")
                number = numbers[-1].text
                dates = soup.find_all(class_="text-left")
                if not dates:
                    raise ValueError("No text-left element found")
                date = pd.to_datetime(dates[-1].text.strip(), format="%d.%m.%Y").date().isoformat()
                tables = soup.find_all("table", attrs={"class":"table centered-table table-bordered"})
                if len(tables) < 4:
                    raise ValueError(f"Expected at least 4 tables, found {len(tables)}")
                table_body = tables[3].find("tbody")
                rows = table_body.find_all("tr") if table_body else []
                if not rows:
                    raise ValueError("No rows in table body")
                table_row = rows[0]
                cells = table_row.find_all("td")
                if len(cells) < 2:
                    raise ValueError("Not enough cells in table row")
                table_date = cells[0]
                table_price = cells[1]

                df2_filled.loc[i, 'raw_number'] = number
                df2_filled.loc[i, 'price as of'] = date
                df2_filled.loc[i, 'date_raw'] = table_date.text.strip()
                df2_filled.loc[i, 'raw_number2'] = table_price.text.strip()

            else:
                pass

        except Exception as e:
            print(f"ERROR for {v}: {e}")
            errors.append((v, str(e)))
            continue


    for j, k in tqdm(enumerate(df2_filled['raw_number']), total=df2_filled.shape[0]):
        if k:
            try:
                price = float(k.strip().replace(',','').replace(' ',''))
                df2_filled.loc[j, 'PRICE'] = price
                try:
                    price2 = float(df2_filled.loc[j, 'raw_number2'].strip().replace(',','.').replace(' ',''))
                    df2_filled.loc[j, 'PRICE2'] = price2
                except:
                    df2_filled.loc[j, 'PRICE2'] = float(df2_filled.loc[j, 'raw_number2'])
                date2 = pd.to_datetime(df2_filled.loc[j, "date_raw"], format="%Y.%m.%d").date().isoformat()
                df2_filled.loc[j, 'date2'] = date2
            except Exception as e:
                isin = df2_filled.loc[j, 'ISIN']
                print(f"ERROR processing row {j} ({isin}): {e}")
                errors.append((isin, f"Post-processing failed: {e}"))


    df2_filled[''] = ''
    df2_filled['Security'] = df2_filled['Stock Code']
    df2_filled['as of Date'] = df2_filled['date2']
    df2_filled['as of Date'] = df2_filled['as of Date'].apply(
        lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%m/%d/%Y") if isinstance(d, str) and d != 'N/A' and d != '0' else d
    )
    df2_filled['Value'] = df2_filled['PRICE2']

    df2_filled = df2_filled.drop('raw_number', axis=1)
    df2_filled = df2_filled.drop('raw_number2', axis=1)
    df2_filled = df2_filled.drop('date_raw', axis=1)
    df2_filled = df2_filled.drop('date2', axis=1)
    df2_filled = df2_filled.drop('PRICE2', axis=1)

    return df2_filled, errors

def convert_df(df):
    return df.to_csv(index=None).encode('utf-8')

st.title('Testing2')

start_button = st.button("UZ all stocks")
start_button2 = st.button("UZ stocks without OTC")

if start_button:
    try:
        with st.spinner("Please wait..."):
            df, errors = get_latest2()
        if errors:
            with st.expander(f"⚠️ {len(errors)} stock(s) failed — click to see details"):
                for isin, msg in errors:
                    st.write(f"- **{isin}**: {msg}")
        st.success(f"Done. {len(df)} stocks loaded, {len(errors)} failed.")

        for col in df.columns:
            df[col] = df[col].apply(str)

        st.dataframe(df)

        csv_file = convert_df(df)

        download_button = st.download_button(label="Download File",
        data = csv_file,
        file_name='latest.csv',
        mime='text/csv')
    except Exception as e:
        st.error(f"Scrape failed: {e}")

if start_button2:
    try:
        with st.spinner("Please wait..."):
            df, errors = get_latest3()
        if errors:
            with st.expander(f"⚠️ {len(errors)} stock(s) failed — click to see details"):
                for isin, msg in errors:
                    st.write(f"- **{isin}**: {msg}")
        st.success(f"Done. {len(df)} stocks loaded, {len(errors)} failed.")

        for col in df.columns:
            df[col] = df[col].apply(str)

        st.dataframe(df)

        csv_file = convert_df(df)

        download_button = st.download_button(label="Download File",
        data = csv_file,
        file_name='latest.csv',
        mime='text/csv')
    except Exception as e:
        st.error(f"Scrape failed: {e}")
