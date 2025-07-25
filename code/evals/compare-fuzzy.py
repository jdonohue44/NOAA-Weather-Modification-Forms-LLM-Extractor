import pandas as pd
import difflib

result = '../../dataset/test/july/july-golden-200-o3.csv'
golden = '../../goldens-for-accuracy-evals/golden-datasets/july/golden-200.csv'
key = 'filename'

def read_csv_fallback(path, **kwargs):
    """
    Try UTF-8 first; on UnicodeDecodeError, report file and line,
    then retry with latin-1.
    """
    try:
        return pd.read_csv(path, **kwargs)
    except UnicodeDecodeError as e:
        # Report the error
        print(f"\n❌ UnicodeDecodeError reading {path}: {e}")
        print("→ Scanning file to locate the bad line…")
        # Scan line by line in binary to find the first offending line
        with open(path, 'rb') as f:
            for lineno, raw in enumerate(f, start=1):
                try:
                    raw.decode('utf-8')
                except UnicodeDecodeError:
                    snippet = raw[:50].replace(b'\n', b'\\n').replace(b'\r', b'\\r')
                    print(f"  • Problem at line {lineno}: {snippet!r}")
                    break
        print("→ Retrying read with encoding='latin-1'.\n")
        return pd.read_csv(path, encoding='latin-1', **kwargs)

purpose_concepts = [
    ['rain', 'rainfall', 'precipitation'],
    ['snow', 'snowfall', 'snowpack', 'winter precipitation', 'precipitation'],
    ['runoff', 'inflow'],
    ['hail'],
    ['fog'],
    ['research']
]

agent_concepts = [
    ['silver iodide', 'agi', 'silver iodate', 'glaciogenic pyrotechnics'],
    ['air', 'ionization', 'shock wave'],
    ['carbon dioxide', 'co2'],
    ['calcium chloride', 'caci2'],
    ['acetone', 'acetone mixture'],
    ['ammonium iodide'],
    ['water droplets', 'water'],
    ['dry ice', 'dry ice pellets']
]

control_area_concepts = [
    ['none', 'na', '', 'no control', 'n/a', 'not specified']
]

operator_concepts = [
    ['weather modification incorporated', 'weather modification llc', 'weather modification inc'],
    ['water enhancement', 'water enhancement authority']
]

def concept_match(text1, text2, concept_groups):
    text1 = text1.lower()
    text2 = text2.lower()
    for group in concept_groups:
        if any(word in text1 for word in group) and any(word in text2 for word in group):
            return True
    return False

def fuzzy_match(val, choices, threshold=0.75):
    match = difflib.get_close_matches(val, choices, n=1, cutoff=threshold)
    return match[0] if match else None

def compute_field_accuracy(output_csv, golden_csv, key='filename', verbose=False):
    df_out  = read_csv_fallback(output_csv)
    df_gold = read_csv_fallback(golden_csv)

    df_out[key] = df_out[key].astype(str)
    df_gold[key] = df_gold[key].astype(str)

    df_out = df_out.set_index(key)
    df_gold = df_gold.set_index(key)

    fields = df_gold.columns
    accuracies = {}

    for field in fields:
        total = 0
        correct = 0

        for count, idx in enumerate(df_gold.index):
            # FOR TESTING BEFORE FINISHING GOLDEN ANNOTATIONS 
            # if count >= 150: 
            #     break
            if idx not in df_out.index:
                continue

            out_val = str(df_out.at[idx, field]).strip().lower()
            gold_val = str(df_gold.at[idx, field]).strip().lower()

            total += 1
            match = False

            if not gold_val and not out_val:
                match = True

            elif field == 'year':
                try:
                    if abs(int(out_val) - int(gold_val)) <= 1:
                        match = True
                except:
                    pass

            elif field == 'season':
                out_set = set(s.strip() for s in out_val.split(',') if s.strip())
                gold_set = set(s.strip() for s in gold_val.split(',') if s.strip())
                if out_set & gold_set:
                    match = True

            elif field in ['state', 'apparatus', 'project', 'operator_affiliation', 'target_area', 'control_area']:
                gold_items = [s.strip() for s in gold_val.split(',')]
                out_items = [s.strip() for s in out_val.split(',')]
                for o in out_items:
                    if fuzzy_match(o, gold_items):
                        match = True
                        break

            elif field == 'purpose':
                if concept_match(out_val, gold_val, purpose_concepts):
                    match = True

            elif field == 'agent':
                if concept_match(out_val, gold_val, agent_concepts):
                    match = True
            
            # elif field == 'control_area':
            #     if concept_match(out_val, gold_val, control_area_concepts):
            #         match = True
            
            # elif field == 'operator_affiliation':
            #     if concept_match(out_val, gold_val, operator_concepts):
            #         match = True

            elif field in ['start_date', 'end_date']:
                if out_val == gold_val:
                    match = True

            else:
                if out_val == gold_val:
                    match = True

            if match:
                correct += 1
            elif verbose:
                print(f"Mismatch in field '{field}' at '{idx}': output='{out_val}' vs gold='{gold_val}'")

        accuracy = correct / total if total else 0
        accuracies[field] = accuracy
        print(f"  -{field:25s}: {accuracy:.2%}")

    overall_accuracy = sum(accuracies.values()) / len(accuracies)
    print(f"\nOverall Average Accuracy: {overall_accuracy:.2%}")

    print("\nSummary Table:")
    summary_df = pd.DataFrame(list(accuracies.items()), columns=['Field', 'Accuracy'])
    print(summary_df.to_string(index=False))

    return accuracies

if __name__ == '__main__':
    compute_field_accuracy(result, golden, key=key, verbose=True)
