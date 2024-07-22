import pandas as pd
import numpy as np
from glob import glob
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


def clean_data(data):
    new_name = {
        0: 'Token',
        2: 'time_entry',
        4: 'open',
        5: 'high',
        6: 'low',
        7: 'prev-close',
        8: 'ltp',
    }
    
    data.rename(new_name, axis=1, inplace=True)
    
    # Clean specific columns
    data['Token'] = data['Token'].str[23:-1]
    data['time_entry'] = data['time_entry'].str[16:-1]
    data['open'] = data['open'].str[9:]
    data['high'] = data['high'].str[9:]
    data['low'] = data['low'].str[8:]
    data['prev-close'] = data['prev-close'].str[15:]
    data['ltp'] = data['ltp'].str[8:]
    
    return data

def clean_data_save(today_date_str):
    file_path = f'Y:\{today_date_str}\option_data.txt'
    save_file_path = f'Y:\daily_option_clean_data\{today_date_str}.csv'
    
    # Create or append to CSV file
    chunk_size = 1000000
    header = ['Token', 'time_entry', 'volume', 'open', 'high', 'low', 'prev-close', 'ltp']
    
    # Process chunks
    with pd.read_csv(file_path, header=None, chunksize=chunk_size, encoding='utf-8', usecols=[0, 2, 4, 5, 6, 7, 8]) as reader:
        for chunk in reader:
            data = clean_data(chunk)
            # Append to the CSV
            data.to_csv(save_file_path, mode='a', header=not pd.io.common.file_exists(save_file_path), index=False)
    print('Data Clean Done and save the data: ')
    # Final data is now in the CSV file


def find_ohlc(data, option_type):
    data_list = []
    #print(data.head(20))
    for strike in data['strike_price'].unique():
        filtered_data = data[data['strike_price'] == strike].reset_index(drop=True)
        filtered_data['time_entry'] = pd.to_datetime(filtered_data['time_entry'])
        filtered_data = filtered_data.sort_values(by='time_entry')
        
        if len(filtered_data) == 0:
            continue  # Skip if no data is found for this strike
        
        exp = filtered_data.iloc[0]['Expiry date']
        option_type = filtered_data.iloc[0]['option_type']
        Symbol = filtered_data.iloc[0]['Symbol']

        
        day_high = filtered_data['ltp'].max()
        day_low = filtered_data['ltp'].min()
        day_open = filtered_data.iloc[0]['ltp']
        day_close = filtered_data.iloc[-1]['ltp']
        
        ohlc = {
            'symbol' : Symbol,
            'expiry_date' : exp,
            'strike_price': strike,
            'option_type': option_type,
            'open': day_open,
            'low': day_low,
            'high': day_high,
            'close': day_close
        }
        data_list.append(ohlc)
    
    #print(data_list)
    return data_list

def find_index_ohlc(df):
    data_list = []
    for symbol in df['Symbol'].unique():
        symbol_data = df[df['Symbol'] == symbol].reset_index(drop=True)
        for expiry in symbol_data['Expiry date'].unique():
            exp_data = symbol_data[symbol_data['Expiry date'] == expiry].reset_index(drop=True)
            
            exp_data = exp_data.sort_values(by='time_entry')
            
            day_high = exp_data['ltp'].max()
            day_low = exp_data['ltp'].min()
            
            day_open = exp_data.iloc[0]['ltp']
            day_close = exp_data.iloc[-1]['ltp']
            
            ohlc = {
                'symbol': symbol,
                'expiry': expiry,
                'open': day_open,
                'low': day_low,
                'high': day_high,
                'close': day_close
            }
            data_list.append(ohlc)
            
    return data_list

def data_and_contract_file_read(today_date_str):
    save_file_path = f'Y:\daily_option_clean_data\{today_date_str}.csv'
    data = pd.read_csv(save_file_path)
    data.dropna(inplace = True)
    data['Token'] = data['Token'].astype(int).astype(str)
    base_dir = r'Y:\separate_option_data'
    contract_file = pd.DataFrame(glob(rf'Y:\daily_contract_file\{today_date_str}\*') , columns = ['file_path'])
    excluded_paths = [
        f'Y:\daily_contract_file\{today_date_str}\contract_{today_date_str}.txt',
        f'Y:\\daily_contract_file\\{today_date_str}\\future_index.csv'
        ]
    
    contract_option_file = contract_file[~contract_file['file_path'].isin(excluded_paths)].reset_index(drop = 'First')
    index_contract_file = contract_file[contract_file['file_path'] == f'Y:\\daily_contract_file\\{today_date_str}\\future_index.csv'].reset_index(drop=True)
    return data,contract_option_file,index_contract_file ,base_dir

def option_save(data, option_contract_path, base_dir, today_date_str):
    data['Token'] = data['Token'].astype(str)
    final_appended_data = pd.DataFrame()

    for path in option_contract_path['file_path']:
        print(f"Processing path: {path}")
        contract_data = pd.read_csv(path)
        df = contract_data[['Token', 'Symbol', 'Expiry date', 'strike_price', 'option_type']]
        df['Token'] = df['Token'].astype(str)
        filter_token = [str(i) for i in df['Token']]

        token_wise_data = data[data['Token'].isin(filter_token)].reset_index(drop=True)
        final_data = pd.merge(token_wise_data, df, on='Token', how="inner")

        if not final_data.empty:
            final_data.drop(['Token'], axis=1, inplace=True)
            exp_name = final_data.iloc[0]['Expiry date']
            index_name = final_data.iloc[0]['Symbol']
            
            df_ce = final_data[final_data['option_type'] == 'CE'].reset_index(drop=True)
            df_pe = final_data[final_data['option_type'] == 'PE'].reset_index(drop=True)

            ce_data_list = find_ohlc(data=df_ce, option_type='CE')
            pe_data_list = find_ohlc(data=df_pe, option_type='PE')
            
            ce_data = pd.DataFrame(ce_data_list)
            pe_data = pd.DataFrame(pe_data_list)
    
            final_appended_data = pd.concat([final_appended_data, ce_data, pe_data], ignore_index=True)
        else:
            print('No data found for this contract.')

    # Create folder and save final_appended_data
    current_date = today_date_str
    new_folder = os.path.join(base_dir, current_date)
    os.makedirs(new_folder, exist_ok=True)

    file_path = os.path.join(new_folder, 'options_data.csv')
    final_appended_data.to_csv(file_path, index=False)
    print(f'Data saved: {file_path}')

def future_data_save(data, index_contract_file, base_dir , today_date_str):
    # Ensure base_dir exists
    os.makedirs(base_dir, exist_ok=True)
    
    # Define current date in the format 'DDMMYYYY'
    current_date = today_date_str
    
    # Create folder for current date
    new_folder = os.path.join(base_dir, current_date)
    os.makedirs(new_folder, exist_ok=True)
    
    # Define file path for saving future data
    file_path = os.path.join(new_folder, 'Future_data.csv')
    
    try:
        # Read index contract file path from index_contract_file DataFrame
        index_contract_path = index_contract_file.iloc[0]['file_path']
        
        # Read relevant columns from index contract CSV
        fut_token = pd.read_csv(index_contract_path, usecols=['Token', 'Instrument', 'Symbol', 'Expiry date'])
        
        # Convert 'Token' to string type to ensure consistency
        fut_token['Token'] = fut_token['Token'].astype(str)
        
        # Filter data based on tokens present in fut_token
        df_filtered = data[data['Token'].isin(fut_token['Token'])].reset_index(drop=True)
        
        # Merge filtered data with fut_token on 'Token' column
        final_data = pd.merge(df_filtered, fut_token, on='Token', how='inner')
        
        # Drop 'Token' column as it's no longer needed
        final_data.drop(['Token'], axis=1, inplace=True)
        
        final_data = find_index_ohlc(df = final_data )
        final_data = pd.DataFrame(final_data)
        
        # Save final data to CSV file
        final_data.to_csv(file_path, index=False)
        
        print(f"Data saved successfully to: {file_path}")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")

if __name__ == '__main__':
    today_date = datetime.today().date()
    today_date_str = today_date.strftime("%d%m%Y")
    print('Data Clean Under process wait 15/20 Mins.......')
    clean_data_save(today_date_str)
    data, contract_option_file, index_contract_file, base_dir = data_and_contract_file_read(today_date_str=today_date_str)
    option_save(data=data, option_contract_path=contract_option_file, base_dir=base_dir, today_date_str=today_date_str)
    future_data_save(data, index_contract_file=index_contract_file, base_dir=base_dir, today_date_str=today_date_str)