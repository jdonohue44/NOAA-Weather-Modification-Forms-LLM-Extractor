import pandas as pd
import difflib

result = '../../dataset/test/golden-50-o4-mini-prompt-C.csv'
golden = '../../accuracy-evals/golden-50.csv'
key = 'filename'

# Concept groups for purpose matching
purpose_concepts = [
    ['rain', 'rainfall', 'precipitation'],
    ['snow', 'snowfall', 'snowpack', 'winter precipitation'],
    ['runoff', 'inflow'],
    ['hail'],
    ['fog'],
    ['research']
]

# Concept groups for purpose matching
agent_concepts = [
    ['silver iodide', 'agi', 'silver iodate'],
    ['air', 'ionization', 'shock wave'],
    ['carbon dioxide', 'co2'],
    ['calcium chloride', 'caci2'],
    ['acetone', 'acetone mixture'],
    ['ammonium iodide'],
    ['water droplets'],
    ['dry ice', 'dry ice pellets']
]

# Concept match
def concept_match(text1, text2, concept_groups):
    text1 = text1.lower()
    text2 = text2.lower()
    for group in concept_groups:
        if any(word in text1 for word in group) and any(word in text2 for word in group):
            return True
    return False

def season_match(output_val, gold_val):
    # Normalize and split both strings into sets
    output_set = set(s.strip().lower() for s in str(output_val).split(','))
    gold_set = set(s.strip().lower() for s in str(gold_val).split(','))
    
    # Return True if there is any overlap
    return bool(output_set & gold_set)

# Fuzzy matching helper
def fuzzy_match(val, choices, threshold=0.8):
    match = difflib.get_close_matches(val, choices, n=1, cutoff=threshold)
    return match[0] if match else None

def compute_field_accuracy(output_csv, golden_csv, key='filename', verbose=False):
    df_out = pd.read_csv(output_csv)
    df_gold = pd.read_csv(golden_csv)

    df_out[key] = df_out[key].astype(str)
    df_gold[key] = df_gold[key].astype(str)

    df_out = df_out.set_index(key)
    df_gold = df_gold.set_index(key)

    fields = df_gold.columns
    accuracies = {}

    for field in fields:
        total = 0
        correct = 0

        for idx in df_gold.index:
            if idx not in df_out.index:
                continue

            out_val = str(df_out.at[idx, field]).strip().lower()
            gold_val = str(df_gold.at[idx, field]).strip().lower()

            if not gold_val:
                continue  # skip if no ground truth

            total += 1

            match = False

            # Special rules by field
            if field == 'year':
                try:
                    out_year = int(out_val)
                    gold_year = int(gold_val)
                    if abs(out_year - gold_year) <= 1:
                        match = True
                except:
                    pass

            elif field == 'season':
                out_set = set(s.strip().lower() for s in str(out_val).split(',') if s.strip())
                gold_set = set(s.strip().lower() for s in str(gold_val).split(',') if s.strip())
                # Direct overlap match
                if out_set & gold_set:
                    match = True

            elif field == 'state':
                gold_items = [s.strip() for s in gold_val.split(',')]
                out_items = [s.strip() for s in out_val.split(',')]
                for o in out_items:
                    if fuzzy_match(o, gold_items):
                        match = True
                        break

            elif field == 'apparatus':
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

            else:
                if out_val == gold_val:
                    match = True

            if match:
                correct += 1
            elif verbose:
                print(f"Mismatch in field '{field}' at '{idx}': output='{out_val}' vs gold='{gold_val}'")

        accuracy = correct / total if total else 0
        accuracies[field] = accuracy

        print(f"  -{field:20s}: {accuracy:.2%}")
        


    overall_accuracy = sum(accuracies.values()) / len(accuracies)
    print(f"\nOverall Average Accuracy: {overall_accuracy:.2%}")

    print("\nSummary Table:")
    summary_df = pd.DataFrame(list(accuracies.items()), columns=['Field', 'Accuracy'])
    print(summary_df.to_string(index=False))

    return accuracies

if __name__ == '__main__':
    compute_field_accuracy(result, golden, key=key, verbose=True)
