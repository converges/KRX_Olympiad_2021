import datetime
import sqlite3
#
import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import ttest_ind as ttest
#
import __config__
import __msg__
import __url__
#
#
class Format:
    # Date-Formatting
    def to_yyyymmdd(date):
        return pd.to_datetime(date).strftime("%Y%m%d")
    def to_yyyy_mm_dd(date):
        return pd.to_datetime(date).strftime("%Y-%m-%d")
    def slash_to_dash(date):
        return date.replace('/', '-')
    # Ticker-Formatting
    def to_A_plus_6digits(ticker): # format: A000000
        return "A" + "0"*(6-len(ticker)) + ticker
    # Number-Formatting
    def removeCommas(number):
        return float(number.replace(',', ''))
    # Stock-Type-Formatting
    def stock_type_formatting(stock_type):
        if stock_type in __config__.COMM_STOCK_TYPES:
            return 'comm'
        elif stock_type in __config__.PREF_STOCK_TYPES:
            return 'pref'
        else:
            print(__msg__.NO_STOCK_TYPE + stock_type)
            return None
    #
#
#
class Database:
    con = sqlite3.connect(__config__.main_db)
    #
    def disconnectDB(self, commit=False):
        ''' Close and commit the sqlite3.connection_db.''' 
        if commit:
            self.con.commit()
        self.con.close()
    #
    def get_tickers(self, option='all'): # Going to add all / kospi / kosdaq
        ''' return all tickers'''
        if option == 'ALL' or option == 'all':
            df_tickers = pd.read_sql(f"SELECT ticker FROM {__config__.BASIC_TICKER_INFO_TABLE}", self.con)
        elif option == 'KS' or option == 'ks':
            df_tickers = pd.read_sql(f"SELECT ticker FROM {__config__.BASIC_TICKER_INFO_TABLE} WHERE market='KOSPI'", self.con)
        elif option == 'KQ' or option == 'kq':
            df_tickers = pd.read_sql(f"SELECT ticker FROM {__config__.BASIC_TICKER_INFO_TABLE} WHERE market='KOSDAQ'", self.con)
        else:
            print(__msg__.NO_OPTIONS + option)
            return -1

        tickers = df_tickers['ticker'].to_list()
        return tickers, len(tickers)
    #
    def ERROR_NO_DATABASE(self, db_name):
        print(__msg__.NO_DATABASE + db_name)
#
#
class TickerDB(Database):
    def __init__(self):
        try:
            self.dataframe = pd.read_sql(f"SELECT * FROM {__config__.BASIC_TICKER_INFO_TABLE}", self.con, index_col='ticker')
        except:
            self.dataframe = None
            self.errorMessage_no_database(db_name=__config__.BASIC_TICKER_INFO_TABLE)
    #
    def create_db(self, delisted_date='20081020'):
        '''
        Download basic information for each ticker.
        Crawls it from KRX data center.
        '''
        
        # 1. Current Listed Tickers / Delisted Tickers
        for is_listed in ['listed', 'delisted']:

            request_url = __url__.krx_ticker_request_url[is_listed]
            request_data = __url__.krx_ticker_request_data[is_listed]

            response = requests.post(request_url, data=request_data)

            # Data-preprocessing
            #__ (1) response --> dataframe
            prep1_str = response.text # str
            prep2_dict = eval(prep1_str) # keys() = ['OutBlock_1', 'CURRENT_DATETIME']: listed
                                         # keys() = ['output', 'CURRENT_DATETIME']: delisted
            if is_listed == 'listed':
                prep3_list = prep2_dict['OutBlock_1']
            elif is_listed == 'delisted':
                prep3_list = prep2_dict['output']

            total_rows = len(prep3_list)
            prep4_newlist = list()

            for i, row in enumerate(prep3_list):
                # 
                prep4_newlist.append(list(map(row.get, __url__.krx_ticker_response_cols[is_listed].keys())))

                print(f"Processing {i+1}/{total_rows}...")


            if is_listed == 'listed':
                prep5_df = pd.DataFrame(prep4_newlist, columns=__url__.krx_ticker_response_cols[is_listed].values())
                prep5_df['delisted_date'] = datetime.datetime(2100, 12, 31).strftime('%Y-%m-%d')
            elif is_listed == 'delisted':
                prep5_df = prep5_df.append(pd.DataFrame(prep4_newlist, columns=__url__.krx_ticker_response_cols[is_listed].values()))


        # 2. Dataframe: Data-preprocessing

        #__ (1) Formats each column.
        prep5_df['ticker'] = prep5_df['ticker'].apply(Format.to_A_plus_6digits)
        prep5_df['type'] = prep5_df['type'].apply(Format.stock_type_formatting)
        prep5_df.drop(prep5_df.loc[prep5_df['type']=='others'].index, inplace=True)

        prep5_df['listed_date'] = prep5_df['listed_date'].apply(Format.slash_to_dash).apply(Format.to_yyyy_mm_dd)
        prep5_df['delisted_date'] = prep5_df['delisted_date'].apply(Format.slash_to_dash).apply(Format.to_yyyy_mm_dd)

        #__ (2) Sets the index column.
        prep5_df.set_index('ticker', drop=True, inplace=True)

        #__ (3) Exports the dataframe as a sqlite file.
        prep5_df.to_sql(__config__.BASIC_TICKER_INFO_TABLE, self.con, if_exists='replace')

        print(prep5_df)
        print(__msg__.COMPLETED_CREATE_DB + __config__.BASIC_TICKER_INFO_TABLE)
#
#
class SharesDB(Database):
    def __init__(self):
        try:
            self.dataframe = pd.read_sql(f"SELECT * FROM {__config__.SHARES_QTY_TABLE}", self.con, index_col='date')
        except pd.io.sql.DatabaseError:
            self.dataframe = None
            self.ERROR_NO_DATABASE(db_name=__config__.SHARES_QTY_TABLE)
#
#
class LendingBalanceDB(Database):
    def __init__(self):
        try:
            self.dataframe = pd.read_sql(f"SELECT * FROM {__config__.LENDING_BALANCE_TABLE}", self.con, index_col='date')
        except pd.io.sql.DatabaseError:
            self.dataframe = None
            self.ERROR_NO_DATABASE(db_name=__config__.LENDING_BALANCE_TABLE)

    def create_db(self):
        ''' Crawling lending-borrowing data from the KOFIA as a json. ''' 

        # KOFIA provides Lending Balance Data from 2008-10-20.
        self.start = datetime.datetime(2008, 10, 20)
        self.end = datetime.datetime.today()

        # (1) Downloads data for each date.
        date_range = pd.date_range(start=self.start, end=self.end, freq="B")

        # KOFIA-lendingBalance: requires a json.
        request_url = __url__.kofia_lendingbalance_request_url
        request_json = __url__.kofia_lendingbalance_request_json

        prep3_alldays_list = list()

        # 2008-10-20 to today
        for day in date_range:
            day = Format.to_yyyy_mm_dd(day)

            # (2) Requests data
            request_json["dmSearch"]["tmpV45"] = Format.to_yyyymmdd(day)
            response = requests.post(request_url, json=request_json)

            # keep from bad requests: (e.g.) Korean Holidays.
            if response.status_code != 200:
                print(f"Skipped {day}...")
                continue
            
            # (3) Data Preprocessing
            prep1_dict = eval(response.text) # keys() = ['unit', 'ds1', 'dsmHeader']
            prep2_list = prep1_dict['ds1']

            for row in prep2_list:
                prep3_alldays_list.append([day] + list(map(row.get, __url__.kofia_lendingbalance_response_cols.keys())))

            print(f"Processing {day}...")

        prep4_df = pd.DataFrame(prep3_alldays_list, columns=['date']+list(__url__.kofia_lendingbalance_response_cols.values()))

        prep4_df.set_index('date', drop=True, inplace=True)
        prep4_df['ticker'] = prep4_df['ticker'].apply(Format.to_A_plus_6digits)

        print(prep4_df)
        prep4_df.to_sql(__config__.LENDING_BALANCE_TABLE, self.con, if_exists='replace')


class shortBalanceDB(Database):
    def __init__(self):
        try:
            self.dataframe = pd.read_sql(f"SELECT * FROM {__config__.SHORT_BALANCE_TABLE}", self.con, index_col='date')
        except pd.io.sql.DatabaseError:
            self.dataframe = None
            self.errorMessage_no_database(db_name=__config__.SHORT_BALANCE_TABLE)

    def create_db(self):
        request_url = __url__.krx_short_balance_request_url
        request_data = __url__.krx_short_balance_request_data

        start = datetime.datetime(2008, 10, 20)
        end = datetime.datetime.today()

        prep4_newlist = list()
        
        for day in pd.date_range(start, end, freq='B'):
            day = Format.to_yyyy_mm_dd(day)

            request_data['trdDd'] = Format.to_yyyymmdd(day)

            response = requests.post(request_url, data=request_data)

            # Data Preprocessing
            prep1_str = response.text
            prep2_dict = eval(prep1_str)
            prep3_list = prep2_dict['OutBlock_1']

            for row in prep3_list:
                prep4_newlist.append([day] + list(map(row.get, __url__.krx_short_balance_response_cols.keys())))

            print(f"Processing {day}...")

        prep5_df = pd.DataFrame(prep4_newlist, columns=['date'] + list(__url__.krx_short_balance_response_cols.values()))

        prep5_df.set_index('date', drop=True, inplace=True)
        prep5_df['ticker'] = prep5_df['ticker'].apply(Format.to_A_plus_6digits)

        # Adjust number to n ','
        num_cols = list(prep5_df.columns)
        num_cols.remove('ticker')

        for col in num_cols:
            prep5_df[col] = prep5_df[col].apply(Format.remove_commas)

        print(prep5_df)
        prep5_df.to_sql(__config__.SHORT_BALANCE_TABLE, self.con, if_exists='replace')


class KospiDB(Database):
    def __init__(self):
        try:
            self.dataframe = pd.read_sql(f"SELECT * FROM {__config__.KOSPI_TABLE}", self.con, index_col='date')
        except pd.io.sql.DatabaseError:
            self.dataframe = None
            print(__msg__.NO_DATABASE + __config__.KOSPI_TABLE)

    def create_db(self, start_date='20081020', end_date=datetime.datetime.today().strftime("%Y%m%d")):
        ''' Create a database with real kospi Index.''' 

        # (1) request data
        request_url = __url__.krx_kospi_request_url
        request_data = __url__.krx_kospi_request_data

        request_data['strtDd'] = start_date
        request_data['endDd'] = end_date

        response = requests.post(url=request_url, data=request_data)

        # (2) Data Preprocessing
        prep1_str = response.text
        prep2_dict = eval(prep1_str) # keys() = ['output', CURRENT_DATETIME']
        prep3_list = prep2_dict['output']
        prep4_newlist = list()

        for row in prep3_list:
            prep4_newlist.append(list(map(row.get, __url__.krx_kospi_response_cols.keys())))
        
        prep5_df = pd.DataFrame(prep4_newlist, columns=__url__.krx_kospi_response_cols.values())

        prep5_df['date'] = prep5_df['date'].apply(Format.to_yyyy_mm_dd)
        prep5_df.set_index('date', drop=True, inplace=True)

        num_cols = list(prep5_df.columns)

        for col in num_cols:
            prep5_df[col] = prep5_df[col].apply(Format.remove_commas)

        print(prep5_df)
        prep5_df.to_sql(__config__.KOSPI_TABLE, self.con, if_exists='replace')
#
#
class DailyStockDB(Database): #KoSPI
    def __init__(self):
        try:
            self.dataframe = pd.read_sql(f"SELECT * FROM {__config__.DAILY_STOCK_TABLE}", self.con, index_col='date')
        except pd.io.sql.DatabaseError:
            print(f"No Database: {__config__.DAILY_STOCK_TABLE}. Run 'create_db' first")
            self.dataframe = None
    #
    def create_db(self):
        self.dataframe = None
        # Gets data 1. the number of shares 2. lending-borrowing balance each.
        try:
            df_shares = pd.read_sql(f"SELECT * FROM {__config__.SHARES_QTY_TABLE}", self.con, index_col='date')
            df_lent = pd.read_sql(f"SELECT * FROM {__config__.LENDING_BALANCE_TABLE}", self.con, index_col='date')
            df_short = pd.read_sql(f"SELECT * FROM {__config__.SHORT_BALANCE_TABLE}", self.con, index_col='date')
        except pd.io.sql.DatabaseError:
            self.errorMessage_no_database(db_name=f"{__config__.SHARES_QTY_TABLE} & {__config__.LENDING_BALANCE_TABLE} & {__config__.SHORT_BALANCE_TABLE}")

        df_short.drop(columns='trdQty', inplace=True)

        kospi_tickers, kospi_total = self.get_tickers(option='ks')
        kosdaq_tickers, kosdaq_total = self.get_tickers(option='kq')
    
        # merge the database ticker-by-ticker.
        for i, (tickers, total) in enumerate(zip([kospi_tickers, kosdaq_tickers], [kospi_total, kosdaq_total])):
            for j, ticker in enumerate(tickers):

                # To merge dataframes into a dataframe by each ticker.
                df_each_shares = df_shares[df_shares['ticker'] == ticker]
                df_each_lent = df_lent[df_lent['ticker'] == ticker]
                df_each_short = df_short[df_short['ticker'] == ticker]

                if df_each_lent.empty or df_each_shares.empty or df_each_short.empty:
                    print(f"No database: {ticker}.")
                    continue
                # Drop the overlapping column named 'ticker'
                df_each_lent.drop(columns='ticker', inplace=True)
                df_each_short.drop(columns='ticker', inplace=True)

                df_merged = pd.concat([df_each_shares, df_each_lent, df_each_short], axis='columns', join='inner')

                if i==0:
                    df_merged.insert(loc=1, column='market', value='KOSPI')
                else:
                    df_merged.insert(loc=1, column='market', value='KOSDAQ')

                # Make a whole dataframe.
                if i==0 and j==0:
                    self.dataframe = df_merged
                else:
                    self.dataframe = self.dataframe.append(df_merged)


                print(f"Processing {j+1}/{total}...")

        # Create columns needed.
        self.dataframe['adjShares_L'] = self.dataframe['shares_L'] / self.dataframe['adjFactor']
        self.dataframe['adjShares_O'] = self.dataframe['shares_O'] / self.dataframe['adjFactor']
        self.dataframe['adjShares_F'] = self.dataframe['shares_F'] / self.dataframe['adjFactor']

        self.dataframe['mktcap_L'] = self.dataframe['price'] * self.dataframe['shares_L']
        self.dataframe['mktcap_O'] = self.dataframe['price'] * self.dataframe['shares_O']
        self.dataframe['mktcap_F'] = self.dataframe['price'] * self.dataframe['shares_F']
        
        self.dataframe['lbr_L'] = self.dataframe['balanceQty'] / self.dataframe['shares_L']
        self.dataframe['lbr_O'] = self.dataframe['balanceQty'] / self.dataframe['shares_O']
        self.dataframe['lbr_F'] = self.dataframe['balanceQty'] / self.dataframe['shares_F']

        self.dataframe['priceRet'] = np.log(self.dataframe['adjPrice'] / self.dataframe['adjPrice'].shift(+1))
        self.dataframe.fillna(0, inplace=True)
        
        # Save the dataframe as a table.
        print(self.dataframe)
        self.dataframe.to_sql(__config__.DAILY_STOCK_TABLE, self.con, if_exists='replace')

    def append_estimated_kospi(self):
        date_range = self.dataframe.index.unique().tolist()

        cols = self.dataframe.columns

        print(date_range)

        new_table = list()
        for day in date_range:
            day = Format.to_yyyy_mm_dd(day)

            new_row = dict()

            for col in cols:
                if col == 'ticker':
                    new_row[col] = 'Estimated_Kospi'
                elif col == 'adjFactor':
                    new_row[col] = 1.0
                elif col == 'lbr_L':
                    new_row[col] = None
                elif col == 'lbr_O':
                    new_row[col] = None
                elif col == 'lbr_F':
                    new_row[col] = None
                elif col == 'priceRet':
                    new_row[col] = None
                elif col == 'shortTrdRatio':
                    new_row[col] = None
                elif col == 'shortAmtRatio':
                    new_row[col] = None
                else:
                    new_row[col] = self.dataframe[col].sum()

            new_table.append(new_row)

            print(f"Processing {day}...")
        new_df = pd.DataFrame(new_table, columns=cols)
        
        new_row['lbr_L'] = new_row['balanceQty'] / new_row['shares_L']
        new_row['lbr_O'] = new_row['balanceQty'] / new_row['shares_O']
        new_row['lbr_F'] = new_row['balanceQty'] / new_row['shares_F']

        new_row['priceRet'] = np.log(new_row['adjPrice'] / new_row['adjPrice'].shift(1))
        new_row.fillna(0, inplace=True)

        new_row['shortTrdRatio'] = new_row['shortTrdQty'] / new_row['trdQty2']
        new_row['shortAmtRatio'] = new_row['shortTrdAmt'] / new_row['trdAmt']


        print(new_df)

        new_df.to_sql(__config__.DAILY_STOCK_TABLE, self.con, if_exists='append')