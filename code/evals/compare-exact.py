import pandas as pd


result = '../../dataset/test/golden-200-o4-mini-prompt-C.csv'
golden = '../../accuracy-evals/golden-200.csv'
key = 'filename'

def compute_field_accuracy(output_csv: str,
                           golden_csv: str,
                           key_column: str = 'filename'):
    # Load data
    df_out = pd.read_csv(output_csv)
    df_gold = pd.read_csv(golden_csv)

    # Normalize column names (optional but good for robustness)
    df_out.columns = df_out.columns.str.strip().str.lower()
    df_gold.columns = df_gold.columns.str.strip().str.lower()
    key_column = key_column.lower()

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

    if len(common) == 0:
        print("❌ No common files to compare.")
        return {}, 0.0

    # Restrict to common files
    df_out = df_out.loc[common]
    df_gold = df_gold.loc[common]

    # Determine fields to compare
    fields = [col for col in df_out.columns if col in df_gold.columns]
    total = len(common)

    accuracies = {}
    print(f"\nComparing: {output_csv} to {golden_csv}")
    print(f"Number of common files: {total}")
    print("Field-level accuracies:")

    if 'year' in df_out.columns and 'year' in df_gold.columns:
        df_out['year'] = df_out['year'].fillna(0).astype(int).astype(str)
        df_gold['year'] = df_gold['year'].fillna(0).astype(int).astype(str)

    for field in fields:
        if field == "purpose":
            # Keyword-based fuzzy match
            keywords = ["snow", "snowpack", "snowfall", "rain", "precipication", "fog", "hail", "runoff", "research", "rainfall"]
            
            def has_overlap(out_val, gold_val):
                out_keywords = {k for k in keywords if k in out_val}
                gold_keywords = {k for k in keywords if k in gold_val}
                return bool(out_keywords & gold_keywords)  # match if intersection

            matches = [
                has_overlap(str(out).lower(), str(gold).lower())
                for out, gold in zip(df_out[field], df_gold[field])
            ]

            accuracy = sum(matches) / total if total else 0
            accuracies[field] = accuracy

            print(f"\n  -{field:20s}: {accuracy:.2%}")

            # PURPOSE - SHOW FUZZY MISMATCHES
            if accuracy < 1.0:
                print(f"    ↪ Fuzzy mismatches in '{field}':")
                for i, match in enumerate(matches):
                    if not match:
                        fname = common[i]
                        print(f"      - {fname} → output: '{df_out.iloc[i][field]}', golden: '{df_gold.iloc[i][field]}'")

        elif field == "agent":
            # Keyword-based fuzzy match
            keywords = ["silver iodide", "sodium iodide", "calcium chloride", "ammonium iodide", "ammonia iodide", "acetone", "urea", "carbon dioxide", "hygroscopic"]
            
            def has_overlap(out_val, gold_val):
                out_keywords = {k for k in keywords if k in out_val}
                gold_keywords = {k for k in keywords if k in gold_val}
                return bool(out_keywords & gold_keywords)  # match if intersection

            matches = [
                has_overlap(str(out).lower(), str(gold).lower())
                for out, gold in zip(df_out[field], df_gold[field])
            ]

            accuracy = sum(matches) / total if total else 0
            accuracies[field] = accuracy

            print(f"\n  -{field:20s}: {accuracy:.2%}")

            # AGENT TYPE - SHOW FUZZY MISMATCHES
            if accuracy < 1.0:
                print(f"    ↪ Fuzzy mismatches in '{field}':")
                for i, match in enumerate(matches):
                    if not match:
                        fname = common[i]
                        print(f"      - {fname} → output: '{df_out.iloc[i][field]}', golden: '{df_gold.iloc[i][field]}'")

        else:
            out_vals = df_out[field].astype(str).str.strip().str.lower().fillna("")
            gold_vals = df_gold[field].astype(str).str.strip().str.lower().fillna("")
            matches = out_vals.eq(gold_vals)
            accuracy = matches.sum() / total if total else 0
            accuracies[field] = accuracy

            print(f"\n  -{field:20s}: {accuracy:.2%}")

            # SHOW MISMATCHES 
            if accuracy < 1.0:
                mismatched = matches[~matches]
                if not mismatched.empty:
                    print(f"Mismatches in '{field}':")
                    diff_df = pd.DataFrame({
                        'output': df_out[field],
                        'golden': df_gold[field],
                        'match': matches
                    }).loc[~matches]
                    print(diff_df.to_string(index=True))

    overall = sum(accuracies.values()) / len(accuracies) if accuracies else 0.0
    print(f"\nOverall average accuracy: {overall:.2%}\n")

    return accuracies, overall

if __name__ == '__main__':
    compute_field_accuracy(result, golden, key)
