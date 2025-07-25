import pandas as pd

def load_dataset(path):
    return pd.read_csv(path)

def find_duplicates(df):
    """Find rows with duplicate filenames."""
    return df[df.duplicated(subset='filename', keep=False)].sort_values('filename')

def clean_dataset(df):
    """Standardize formatting for Zenodo publishing."""
    df_clean = df.copy()

    # Standardize column names
    df_clean.columns = [col.strip().lower() for col in df_clean.columns]

    # Clean string columns
    str_cols = df_clean.select_dtypes(include='object').columns
    for col in str_cols:
        df_clean[col] = df_clean[col].astype(str).str.strip()

    # Convert date columns to ISO format
    for col in ['start_date', 'end_date']:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce').dt.date

    return df_clean

def save_dataset(df, output_path):
    df.to_csv(output_path, index=False)
    print(f"Cleaned dataset saved to: {output_path}")

def main():
    input_path = "cloud_seeding_us_2000_2025.csv"
    output_path = "cloud_seeding_us_2000_2025_cleaned.csv"

    df = load_dataset(input_path)
    duplicates = find_duplicates(df)

    if not duplicates.empty:
        print("\nFound duplicate filenames:")
        print(duplicates[['filename']].drop_duplicates())
    else:
        print("\nNo duplicate filenames found.")

    df_clean = clean_dataset(df)
    save_dataset(df_clean, output_path)

if __name__ == "__main__":
    main()
