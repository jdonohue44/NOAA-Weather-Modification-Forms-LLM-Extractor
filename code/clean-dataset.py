from evals.concepts import (
    PURPOSE_MAP,
    AGENT_MAP,
    CONTROL_MAP,
    OPERATOR_MAP,
    _slug,             
)
import pandas as pd
import re

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

def _apply_mapping(cell, mapping):
    if pd.isna(cell):
        return pd.NA
    parts = re.split(r'[;,]|\\band\\b|\\b&\\b|\\bplus\\b', str(cell).lower())
    mapped = [mapping.get(_slug(p), _slug(p)) for p in parts if p.strip()]
    # parts = re.split(r'[;,]', str(cell).lower())
    # mapped = [mapping.get(p.strip(), p.strip()) for p in parts if p.strip()]
    # dedupe while preserving order
    seen = set()
    canonical_parts = [t for t in mapped if not (t in seen or seen.add(t))]
    return ', '.join(canonical_parts)

def standardize_semantic_terms(df):
    col_to_map = {
        'purpose': PURPOSE_MAP,
        'agent': AGENT_MAP,
        'control_area': CONTROL_MAP,
        'operator_affiliation': OPERATOR_MAP,
    }
    for col, mp in col_to_map.items():
        if col in df.columns:
            df[col] = df[col].apply(_apply_mapping, args=(mp,))
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
    df = standardize_semantic_terms(df)
    df = normalize_missing_values(df)
    df = parse_dates(df)
    df = remove_duplicates(df)
    df = sort_dataset(df)

    df.to_csv(output_path, index=False)
    print(f"Cleaned dataset saved to: {output_path}")

if __name__ == "__main__":
    input_path = "../dataset/final/cloud_seeding_us_2000_2025.csv"
    output_path = "../dataset/final/cleaned_cloud_seeding_us_2000_2025.csv"
    clean_dataset(input_path, output_path)
