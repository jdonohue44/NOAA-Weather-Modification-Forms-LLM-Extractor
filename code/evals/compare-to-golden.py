import pandas as pd
import difflib
import re

result = '../../dataset/test/july/july-golden-200-o3.cleaned.csv'
golden = '../../goldens-for-accuracy-evals/golden-datasets/july/golden-200.csv'
key = 'filename'

def read_csv_with_fallback(path, **kwargs):
    try:
        return pd.read_csv(path, **kwargs)
    except UnicodeDecodeError as e:
        print(f"\nERROR: UnicodeDecodeError reading {path}: {e}")
        print("→ Scanning file to locate the bad line…")
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
    # snowpack
    ['augment snowpack',
     'increase snowpack',
     'snowpack augmentation',
     'snowpack enhancement'],
    # snowfall
    ['augment snowfall',
     'increase snowfall',
     'snowfall augmentation',
     'snowfall enhancement',
     'snow augmentation',
     'augment snow'],
    # precipitation / rainfall
    ['augment precipitation',
     'increase precipitation',
     'precipitation augmentation',
     'precipitation enhancement',
     'augment rainfall',
     'increase rainfall',
     'rainfall augmentation',
     'rainfall enhancement',
     'augment rain',
     'increase rain',
     'rain augmentation',
     'rain enhancement',
     'rainfall increase',
     'increase precipitation',
     'rain enhancement'],
    # runoff / inflow
    ['increase runoff',
     'augment runoff',
     'runoff',
     'increase inflow',
     'inflow',
     'increase inflow to twitchell reservoir'],
    # hail
    ['suppress hail',
     'hail suppression',
     'hail mitigation',
     'hail damage mitigation'],
    # fog
    ['suppress fog',
     'fog suppression',
     'fog dissipation',
     'dissipate fog'],
    # research
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
    ['none', 'na', 'nan', '', ' ', 'no control', 'n/a', 'not specified'],
    ['same as target area', 'target area', 'adjacent areas', 'surrounding area', 'not a target/control project', 'whichever of the two ranges is not seeded'],
    ['various sites', 'multiple locations', 'regional area']
]

operator_concepts = [
    # Weather Modification Inc
    ['weather modification incorporated', 'weather modification llc', 'weather modification inc'],
    # Water Enhancement Authority
    ['water enhancement', 'water enhancement authority'],
    # Atmospherics Inc
    ['atmospherics inc', 'atmospherics inc.'],
    # Western Weather Consultants
    ['western weather consultants', 'western weather consultants llc'],
    # RHS Consulting
    ['rhs consulting', 'rhs consulting ltd', 'rhs consulting, ltd.'],
    # Pacific Gas and Electric
    ['pacific gas and electric', 'pacific gas and electric company'],
    # Eden Valley Irrigation & Drainage
    ['eden valley irrigation & drainage district', 'eden valley irrigation and drainage'],
    # Franklin Soil & Water Conservation District
    ['franklin soil and water conservation district', 'franklin soil & water conservation district'],
    # High Plains Groundwater District
    ['high plains underground water conservation district 1',
     'high plains underground water conservation district no. 1',
     'high plains underground water conservation district #1'],
    # Western Kansas GMD
    ['western kansas groundwater management district #1',
     'western kansas groundwater management'],
    # Powell Plant Farms
    ['powell plant farms inc', 'powell plant farms, inc.'],
    # Southwest Texas Rain-Enhancement
    ['southwest texas rain-enhancement association', 'southwest texas rain enhancement association'],
    # Belding Farms
    ['belding farms', 'general manager, belding farms'],
    # Clark County
    ['clark county', 'clark county, idaho'],
    # Barken Fog Ops
    ['barken fog ops, inc', 'barken fog ops, inc.'],
    # North Plains Groundwater District
    ['north plains groundwater district', 'north plains groundwater district no. 2']
]

def normalize_date_format(val):
    try:
        return pd.to_datetime(val, errors='coerce').strftime('%-m/%-d/%y')
    except:
        return str(val).strip().lower()

def normalize_tokens(text):
    return set(re.findall(r'\b[a-z0-9]+\b', text.lower()))

def token_overlap(out_val, gold_val, min_overlap_ratio=0.5):
    out_tokens = normalize_tokens(out_val)
    gold_tokens = normalize_tokens(gold_val)
    intersection = out_tokens & gold_tokens
    return len(intersection) >= min_overlap_ratio * len(gold_tokens)

def concept_match(text1, text2, concept_groups):
    text1 = text1.strip().lower()
    text2 = text2.strip().lower()
    for group in concept_groups:
        if fuzzy_match(text1, group) and fuzzy_match(text2, group):
            return True
    return False

def fuzzy_match(val, choices, threshold=0.75):
    match = difflib.get_close_matches(val, choices, n=1, cutoff=threshold)
    return match[0] if match else None

def compute_field_accuracy(output_csv, golden_csv, key='filename', verbose=False):
    df_out  = read_csv_with_fallback(output_csv)
    df_gold = read_csv_with_fallback(golden_csv)

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
            if idx not in df_out.index:
                print(f"MISSING FROM GOLDEN CSV IN RESULT CSV AT ROW INDEX {idx}")
                continue

            out_val = str(df_out.at[idx, field]).strip().lower()
            gold_val = str(df_gold.at[idx, field]).strip().lower()

            total += 1
            match = False
            
            if not gold_val and not out_val: # empty cell is correct if that is what is in the golden dataset
                match = True

            # --- STRUCTURED FIELDS --- #
            elif field == 'year':
                try:
                    if abs(int(out_val) - int(gold_val)) <= 1: # allow +/- a year to handle winter cases
                        match = True
                except:
                    pass

            elif field in ['season','state']:
                if out_val == gold_val:
                    match = True
                out_set = set(s.strip() for s in out_val.split(',') if s.strip())
                gold_set = set(s.strip() for s in gold_val.split(',') if s.strip())
                if out_set & gold_set:
                    match = True
            
            elif field in ['start_date', 'end_date']:
                if normalize_date_format(out_val) == normalize_date_format(gold_val):
                    match = True

            # --- SEMI STRUCTURED FIELDS --- #
            elif field == 'purpose':
                if concept_match(out_val, gold_val, purpose_concepts):
                    match = True
                out_set  = set(s.strip() for s in out_val.split(',')  if s.strip())
                gold_set = set(s.strip() for s in gold_val.split(',') if s.strip())
                if out_set == gold_set:
                    match = True
                else:
                    for o in out_set:
                        if o in gold_set or fuzzy_match(o, list(gold_set)):
                            match = True
                            break
            
            elif field == 'apparatus':
                out_set  = set(s.strip() for s in out_val.split(',')  if s.strip())
                gold_set = set(s.strip() for s in gold_val.split(',') if s.strip())
                if out_set == gold_set:
                    match = True
                else:
                    for o in out_set:
                        if o in gold_set or fuzzy_match(o, list(gold_set)):
                            match = True
                            break
            
            elif field == 'operator_affiliation':
                if out_val == gold_val:
                    match = True
                elif fuzzy_match(out_val, gold_val):
                    match = True
                    break
                elif concept_match(out_val, gold_val, operator_concepts):
                    match = True

            elif field == 'agent':
                if concept_match(out_val, gold_val, agent_concepts):
                    match = True
                out_set  = set(s.strip() for s in out_val.split(',')  if s.strip())
                gold_set = set(s.strip() for s in gold_val.split(',') if s.strip())
                if out_set == gold_set:
                    match = True
                else:
                    for o in out_set:
                        if o in gold_set or fuzzy_match(o, list(gold_set)):
                            match = True
                            break

            # --- LEAST STRUCTURED FIELDS --- #
            elif field in ['project']:
                if out_val == gold_val:
                    match = True
                elif fuzzy_match(out_val, gold_val):
                    match = True
                    break

            elif field == 'target_area':
                if out_val == gold_val:
                    match = True
                elif token_overlap(out_val, gold_val, min_overlap_ratio=0.5):
                    match = True

            elif field == 'control_area':
                if out_val == gold_val:
                    match = True
                elif token_overlap(out_val, gold_val, min_overlap_ratio=0.5):
                    match = True
                elif concept_match(out_val, gold_val, control_area_concepts):
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
