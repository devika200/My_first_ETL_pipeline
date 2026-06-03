import pandas as pd

def load_and_clean_data(csv_path):
    """
    Load the CSV file and do basic preprocessing
    """
    print("Loading CSV file...")
    df = pd.read_csv(csv_path)
    
    print(f"Original row count: {len(df)}")
    
    # Check for nulls
    print("\nNull values per column:")
    print(df.isnull().sum())

    df['TransactionDate'] = (
    pd.to_datetime(df['TransactionDate'])
    .dt.strftime('%Y-%m-%d %H:%M:%S')
)

    df['PreviousTransactionDate'] = (
    pd.to_datetime(df['PreviousTransactionDate'])
    .dt.strftime('%Y-%m-%d %H:%M:%S')
)   
    
    # Convert date columns to datetime
    print("\nConverting date columns...")
    df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])
    df['PreviousTransactionDate'] = pd.to_datetime(df['PreviousTransactionDate'])
    
    # Clean string columns - strip whitespace
    string_cols = ['TransactionID', 'AccountID', 'TransactionType', 'Location', 
                   'DeviceID', 'IP Address', 'MerchantID', 'Channel', 'CustomerOccupation']
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()
    
    # Standardize TransactionType to title case
    df['TransactionType'] = df['TransactionType'].str.title()
    
    # Standardize Channel to title case
    df['Channel'] = df['Channel'].str.title()


    #numeric
    df["TransactionAmount"] = (
    df["TransactionAmount"]
    .round(2)
)

    df["AccountBalance"] = (
        df["AccountBalance"]
        .round(2)
    )
    
    # Check for duplicates
    duplicates = df.duplicated().sum()
    print(f"\nDuplicate rows found: {duplicates}")
    if duplicates > 0:
        df = df.drop_duplicates()
        print(f"Removed duplicates. New row count: {len(df)}")
    
    # Basic data type validation
    print("\nData types after cleaning:")
    print(df.dtypes)
    
    print(f"\nFinal row count: {len(df)}")
    
    return df

if __name__ == "__main__":
    # Test the preprocessing
    df = load_and_clean_data("bank_transactions_data_2.csv")
    print("\nFirst few rows:")
    print(df.head())