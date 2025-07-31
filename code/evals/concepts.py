import re, unicodedata

def _slug(text):
    t = unicodedata.normalize('NFKD', text)
    t = re.sub(r'[^a-z0-9 ]', '', t.lower())
    t = re.sub(r'\b(s|es)\b', '', t)                # crude plural -> singular
    return t.strip()

def _build_map(groups):
    mapping = {}
    for group in groups:
        canonical = _slug(group[0])
        for term in group:
            mapping[_slug(term)] = canonical
    return mapping

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
    ['increase precipitation',
     'augment precipitation',
     'augment winter precipitation',
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
    ['ionized air', 'air', 'ionization', 'shock wave', 'shock waves', 'nan', 'na', '', ' '],
    ['sodium iodide'],
    ['cesium iodide'],
    ['carbon dioxide', 'co2'],
    ['calcium chloride', 'caci2'],
    ['acetone', 'acetone mixture'],
    ['ammonium iodide', 'ammonia iodide'],
    ['water', 'water droplets', 'liquid water', 'sea salt'],
    ['dry ice', 'dry ice pellets'],
    ['hygroscopic']
]

control_area_concepts = [
    ['none', 'na', 'nan', '', ' ', 'no control', 'n/a', 'not specified', 'same as target area', 'target area', 'not a target/control project'],
    ['adjacent areas', 'various sites', 'multiple locations', 'regional area', 'surrounding area', 'whichever of the two ranges is not seeded']
]

operator_concepts = [
    # Weather Modification Inc
    ['weather modification inc', 'weather modification llc', 'weather modification incorporated'],
    # Water Enhancement Authority
    ['water enhancement authority', 'water enhancement'],
    # Atmospherics Inc
    ['atmospherics inc', 'atmospherics inc.', 'atmospherics, inc.'],
    # Western Weather Consultants
    ['western weather consultants llc', 'western weather consultants'],
    # RHS Consulting
    ['rhs consulting ltd', 'rhs consulting', 'rhs consulting, ltd.', 'rhs consulting ltd.'],
    # Pacific Gas and Electric
    ['pacific gas and electric company', 'pacific gas and electric'],
    # Pacific Coast Forecasting
    ['pacific coast forecasting inc', 'pacific coast forecasting', 'pacific coast forecasting inc.'],
    # Eden Valley Irrigation & Drainage
    ['eden valley irrigation and drainage', 'eden valley irrigation & drainage district'],
    # Franklin Soil & Water Conservation District
    ['franklin soil and water conservation', 'franklin soil and water conservation district', 'franklin soil & water conservation district'],
    # High Plains Groundwater District
    ['high plains underground water conservation',
     'high plains underground water conservation district 1',
     'high plains underground water conservation district no. 1',
     'high plains underground water conservation district #1'],
    # Western Kansas GMD
    ['western kansas groundwater management',
     'western kansas groundwater management district #1'],
    # Powell Plant Farms
    ['powell plant farms inc', 'powell plant farms, inc.', 'powell plant farms inc.'],
    # Southwest Texas Rain-Enhancement
    ['southwest texas rain enhancement association', 'southwest texas rain-enhancement association'],
    # Belding Farms
    ['belding farms', 'general manager, belding farms'],
    # Clark County
    ['clark county', 'clark county, idaho'],
    # Barken Fog Ops
    ['barken fog ops inc', 'barken fog ops, inc.', 'barken fog ops, inc', 'barken fog ops inc.'],
    # North Plains Groundwater District
    ['north plains groundwater district', 'north plains groundwater district no. 2'],
    # Transpecos Weather Modification Association
    ['transpecos weather modification association', 'trans-pecos weather modification association']
]

PURPOSE_MAP  = _build_map(purpose_concepts)
AGENT_MAP    = _build_map(agent_concepts)
CONTROL_MAP  = _build_map(control_area_concepts)
OPERATOR_MAP = _build_map(operator_concepts)