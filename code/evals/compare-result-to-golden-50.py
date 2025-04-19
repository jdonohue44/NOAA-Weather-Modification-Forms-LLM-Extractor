import pandas as pd

result = '../../dataset/test/golden-50-gpt-4.1-mini-prompt-A.csv'
golden = '../../accuracy-evals/golden-50.csv'
key = 'filename'

def compute_field_accuracy(output_csv: str,
                           golden_csv: str,
                           key_column: str = 'filename'):

    # Load data
    df_out = pd.read_csv(output_csv)
    df_gold = pd.read_csv(golden_csv)

    # Set index for alignment
    df_out.set_index(key_column, inplace=True)
    df_gold.set_index(key_column, inplace=True)

    # Find common, missing, and extra filenames
    common = df_out.index.intersection(df_gold.index)
    missing = df_gold.index.difference(df_out.index)
    extra = df_out.index.difference(df_gold.index)

    if not missing.empty:
        print(f"⚠️ Missing output for {len(missing)} files: {missing.tolist()}")
    if not extra.empty:
        print(f"⚠️ Extra output files not in golden: {extra.tolist()}")

    # Restrict to common files
    df_out = df_out.loc[common]
    df_gold = df_gold.loc[common]

    # Fields to compare (columns present in both)
    fields = [col for col in df_out.columns if col in df_gold.columns]
    total = len(common)

    # Compute per-field accuracy
    accuracies = {}
    for field in fields:
        matches = (df_out[field] == df_gold[field]).sum()
        accuracies[field] = matches / total

    # Print results
    print("\nField-level accuracies:")
    for field, acc in accuracies.items():
        print(f"  • {field:20s}: {acc:.2%}")

    overall = sum(accuracies.values()) / len(accuracies)
    print(f"\nOverall average accuracy: {overall:.2%}")

    return accuracies, overall

if __name__ == '__main__':
    compute_field_accuracy(result, golden, key)

