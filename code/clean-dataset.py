import pandas as pd

def load_dataset(path):
    return pd.read_csv(
        path,
        na_values=['', 'none', 'n/a', 'na', 'null'],
        keep_default_na=True
    )

def standardize_column_names(df):
    df.columns = [col.strip().lower() for col in df.columns]
    return df

def lowercase_text(df):
    str_cols = df.select_dtypes(include='object').columns
    for col in str_cols:
        if col == 'filename':
            continue  # leave filename untouched
        df[col] = df[col].astype(str).str.strip().str.lower()
    return df

def parse_dates(df):
    for col in ['start_date', 'end_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d').fillna(pd.NA)
    return df

def remove_duplicates(df):
    before = len(df)
    df = df.drop_duplicates(subset='filename', keep='first')
    after = len(df)
    if before > after:
        print(f"Removed {before - after} duplicate rows based on filename.")
    return df

def sort_dataset(df):
    if 'filename' in df.columns:
        df = df.sort_values(by=['filename'])
    return df

def normalize_missing_values(df):
    df.replace(
        to_replace=['', 'none', 'n/a', 'na', 'null'],
        value=pd.NA,
        inplace=True
    )
    return df

def validate_required_columns(df, required=None):
    if required is None:
        required = [
            'filename',
            'project',
            'year',
            'season',
            'state',
            'operator_affiliation',
            'agent',
            'apparatus',
            'purpose',
            'target_area',
            'control_area',
            'start_date',
            'end_date'
        ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df

def clean_dataset(path, output_path):
    df = load_dataset(path)
    df = standardize_column_names(df)
    df = validate_required_columns(df)
    df = lowercase_text(df)
    df = normalize_missing_values(df)
    df = parse_dates(df)
    df = remove_duplicates(df)
    df = sort_dataset(df)

    df.to_csv(output_path, index=False)
    print(f"Cleaned dataset saved to: {output_path}")

if __name__ == "__main__":
    input_path = "final-test-july-golden-200-o3-prompt-D.csv"
    output_path = "cleaned-final-test-july-golden-200-o3-prompt-D.csv"
    clean_dataset(input_path, output_path)
