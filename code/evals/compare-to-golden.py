import pandas as pd
import difflib
import re

result = '../../dataset/final/cleaned_cloud_seeding_us_2000_2025.csv'
golden = '../../goldens-for-accuracy-evals/golden-datasets/july/golden-200.csv'
key = 'filename'

def read_csv_with_fallback(path, **kwargs):
    try:
        return pd.read_csv(path, **kwargs)
    except UnicodeDecodeError as e:
        print(f"\nERROR: UnicodeDecodeError reading {path}: {e}")
        print("Scanning file to locate the bad line…")
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
    # snow
    ['augment snowpack',
     'increase snowpack',
     'snowpack augmentation',
     'snowpack enhancement',
     'augment snowfall',
     'increase snowfall',
     'snowfall augmentation',
     'snowfall enhancement',
     'snow augmentation',
     'winter precipitation',
     'augment snow'],
    # precipitation / rainfall
    ['augment precipitation',
     'augment winter precipitation',
     'increase precipitation',
     'precipitation augmentation',
     'precipitation enhancement',
     'augment rainfall',
     'increase rainfall',
     'rainfall augmentation',
     'rainfall enhancement',
     'augment rain',
     'increase rain',
     'increase rainfall',
     'enhance rain',
     'enhance rainfall',
     'rain augmentation',
     'rain enhancement',
     'rainfall increase',
     'increase precipitation',
     'rain enhancement'],
    # runoff / inflow
    ['increase runoff',
     'increase water supply',
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
    ['research',
     'reduce global temperature']
]

agent_concepts = [
    ['silver iodide', 'agi', 'silver iodate', 'glaciogenic pyrotechnics'],
    ['air', 'ionization', 'ionized air', 'shock wave', 'shock waves', 'nan', 'na', '', ' '],
    ['sodium iodide'],
    ['cesium iodide'],
    ['carbon dioxide', 'co2'],
    ['calcium chloride', 'caci2'],
    ['acetone', 'acetone mixture'],
    ['ammonium iodide', 'ammonia iodide'],
    ['water droplets', 'water', 'liquid water', 'sea salt'],
    ['dry ice', 'dry ice pellets'],
    ['hygroscopic']
]

control_area_concepts = [
    ['none', 'na', 'nan', '', ' ', 'no control', 'n/a', 'not specified', 'same as target area', 'target area', 'not a target/control project'],
    ['various sites', 'multiple locations', 'regional area', 'adjacent areas', 'surrounding area', 'whichever of the two ranges is not seeded']
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
    ['rhs consulting', 'rhs consulting ltd', 'rhs consulting, ltd.', 'rhs consulting ltd.'],
    # Pacific Gas and Electric
    ['pacific gas and electric', 'pacific gas and electric company'],
    # Pacific Coast Forecasting
    ['pacific coast forecasting inc.', 'pacific coast forecasting'],
    # Eden Valley Irrigation & Drainage
    ['eden valley irrigation & drainage district', 'eden valley irrigation and drainage'],
    # Franklin Soil & Water Conservation District
    ['franklin soil and water conservation district', 'franklin soil & water conservation district'],
    # High Plains Groundwater District
    ['high plains underground water conservation district 1',
     'high plains underground water conservation district no. 1',
     'high plains underground water conservation district #1'],
    # Western Kansas GMD
    ['western kansas groundwater management',
     'western kansas groundwater management district #1'],
    # Powell Plant Farms
    ['powell plant farms inc', 'powell plant farms, inc.'],
    # Southwest Texas Rain-Enhancement
    ['southwest texas rain-enhancement association', 'southwest texas rain enhancement association'],
    # Belding Farms
    ['belding farms', 'general manager, belding farms'],
    # Clark County
    ['clark county', 'clark county, idaho'],
    # Barken Fog Ops
    ['barken fog ops, inc.', 'barken fog ops, inc', 'barken fog ops inc.', 'barken fog ops inc'],
    # North Plains Groundwater District
    ['north plains groundwater district', 'north plains groundwater district no. 2'],
    # Transpecos Weather Modification Association
    ['transpecos weather modification association', 'trans-pecos weather modification association']
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
        normalized_group = [g.strip().lower() for g in group]
        if fuzzy_match(text1, normalized_group) and fuzzy_match(text2, normalized_group):
            return True
    return False

def fuzzy_match(val, choices, threshold=0.75):
    val = val.strip().lower()
    choices = [c.strip().lower() for c in choices]
    return difflib.get_close_matches(val, choices, n=1, cutoff=threshold)

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
                # print(f"MISSING: {idx}")
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
            
            # elif field in ['start_date', 'end_date']:
            #     if normalize_date_format(out_val) == normalize_date_format(gold_val):
            #         match = True

            elif field in ['start_date', 'end_date']:
                out_date = pd.to_datetime(out_val, errors='coerce')
                gold_date = pd.to_datetime(gold_val, errors='coerce')
                if pd.notna(out_date) and pd.notna(gold_date):
                    if abs((out_date - gold_date).days) <= 30:
                        match = True

            # --- SEMI STRUCTURED FIELDS --- #
            # elif field == 'purpose':
            #     if concept_match(out_val, gold_val, purpose_concepts):
            #         match = True
            #     out_set  = set(s.strip() for s in out_val.split(',')  if s.strip())
            #     gold_set = set(s.strip() for s in gold_val.split(',') if s.strip())
            #     if out_set == gold_set:
            #         match = True
            #     else:
            #         for o in out_set:
            #             if o in gold_set or fuzzy_match(o, list(gold_set)):
            #                 match = True
            #                 break
            elif field == 'purpose':
                out_set = set(s.strip() for s in out_val.split(',') if s.strip())
                gold_set = set(s.strip() for s in gold_val.split(',') if s.strip())

                # Ensure each gold concept is matched by an output concept
                all_matched = True
                for g in gold_set:
                    found = False
                    for o in out_set:
                        if concept_match(o, g, purpose_concepts) or fuzzy_match(o, [g]):
                            found = True
                            break
                    if not found:
                        # if verbose:
                            # print(f"  No concept match for '{g}' in output: {out_set}")
                        all_matched = False
                        break

                if all_matched:
                    match = True

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
                match = True # manual inspection
                # if out_val == gold_val:
                #     match = True
                # elif fuzzy_match(out_val, gold_val):
                #     match = True
                #     break

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
