### Program Overview

This program processes financial data related to options and futures, cleans it, computes OHLC (Open, High, Low, Close) values, and saves the cleaned and processed data to CSV files. The script performs the following main tasks:
1. **Data Cleaning**
2. **Computing OHLC Values for Options**
3. **Computing OHLC Values for Futures**
4. **Saving Processed Data**

### Functions and Their Responsibilities

1. **clean_data(data)**
   - **Purpose:** Cleans the raw data by renaming columns and extracting relevant parts of the strings.
   - **Input:** A DataFrame `data` containing raw financial data.
   - **Output:** A cleaned DataFrame.

2. **clean_data_save(today_date_str)**
   - **Purpose:** Reads raw data from a file, cleans it using `clean_data()`, and saves the cleaned data to a CSV file.
   - **Input:** `today_date_str` - The date string representing today's date in `DDMMYYYY` format.
   - **Output:** A CSV file containing cleaned data.

3. **find_ohlc(data, option_type)**
   - **Purpose:** Computes OHLC values for each unique strike price in the data.
   - **Input:** 
     - `data` - DataFrame containing options data.
     - `option_type` - String indicating the type of option ('CE' or 'PE').
   - **Output:** A list of dictionaries containing OHLC values.

4. **find_index_ohlc(df)**
   - **Purpose:** Computes OHLC values for each unique symbol and expiry date in the data.
   - **Input:** DataFrame `df` containing index data.
   - **Output:** A list of dictionaries containing OHLC values.

5. **data_and_contract_file_read(today_date_str)**
   - **Purpose:** Reads the cleaned data and contract files.
   - **Input:** `today_date_str` - The date string representing today's date in `DDMMYYYY` format.
   - **Output:** A tuple containing:
     - DataFrame `data` of cleaned data.
     - DataFrame `contract_option_file` with paths to option contract files.
     - DataFrame `index_contract_file` with paths to index contract files.
     - `base_dir` - Base directory for saving processed data.

6. **option_save(data, option_contract_path, base_dir, today_date_str)**
   - **Purpose:** Computes OHLC values for options and saves the results to CSV files.
   - **Input:** 
     - `data` - DataFrame containing cleaned data.
     - `option_contract_path` - DataFrame with paths to option contract files.
     - `base_dir` - Base directory for saving processed data.
     - `today_date_str` - Date string in `DDMMYYYY` format.
   - **Output:** CSV files containing OHLC values for options.

7. **future_data_save(data, index_contract_file, base_dir, today_date_str)**
   - **Purpose:** Computes OHLC values for futures and saves the results to CSV files.
   - **Input:** 
     - `data` - DataFrame containing cleaned data.
     - `index_contract_file` - DataFrame with paths to index contract files.
     - `base_dir` - Base directory for saving processed data.
     - `today_date_str` - Date string in `DDMMYYYY` format.
   - **Output:** CSV files containing OHLC values for futures.

### Main Program Workflow

1. **Get Today's Date:** 
   ```python
   today_date = datetime.today().date()
   today_date_str = today_date.strftime("%d%m%Y")
   print('Data Clean Under process wait 15/20 Mins.......')
   clean_data_save(today_date_str)
   data, contract_option_file, index_contract_file, base_dir = data_and_contract_file_read(today_date_str=today_date_str)
   option_save(data=data, option_contract_path=contract_option_file, base_dir=base_dir, today_date_str=today_date_str)
   future_data_save(data, index_contract_file=index_contract_file, base_dir=base_dir, today_date_str=today_date_str)

### Input Variables

- today_date_str: A string representing today's date in DDMMYYYY format. Used to read and save files.
- data: DataFrame containing cleaned options and futures data.
- option_contract_path: DataFrame containing paths to option contract files.
- index_contract_file: DataFrame containing paths to index contract files.
- base_dir: Base directory for saving processed data.


### Output

- Cleaned Data CSV File: Stored in Y:\daily_option_clean_data\{today_date_str}.csv
- Options OHLC CSV File: Stored in Y:\separate_option_data\{today_date_str}\options_data.csv
- Futures OHLC CSV File: Stored in Y:\separate_option_data\{today_date_str}\Future_data.csv


