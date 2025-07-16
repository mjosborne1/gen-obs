import argparse
import os
import requests
from urllib import parse
import pandas as pd
from fhirclient.models import observation, codeableconcept, coding, quantity
import json
import uuid
from datetime import datetime
from fhirpathpy import evaluate

from utils import check_path
import logging


baseurl="https://r4.ontoserver.csiro.au/fhir"


## get_concept_all_props
## Perform a CodeSystem lookup and get all properties 
## return a json resposne from the curl call
def get_concept_display(code, system):
    cslookup = '/CodeSystem/$lookup'
    query = baseurl + cslookup + '?system=' + parse.quote(system, safe='') + "&code=" + code
    headers = {'Accept': 'application/fhir+json'}
    response = requests.get(query, headers=headers)
    data = response.json()
    # Find the preferred display property
    for param in data.get('parameter', []):
        if param.get('name') == 'display':
            return param.get('valueString')
        if param.get('name') == 'property':
            for part in param.get('part', []):
                if part.get('name') == 'preferred' and part.get('valueBoolean'):
                    # If preferred, get the display
                    for subpart in param.get('part', []):
                        if subpart.get('name') == 'display':
                            return subpart.get('valueString')
    # fallback: try to extract display from fhirpathpy if present
    expr = "Parameters.parameter.where(name='property').part.where(name='display').valueString"
    display = evaluate(data, expr)
    if display:
        return display[0] if isinstance(display, list) else display
    return None

def create_observations(srcfile, outdir):
    """
    Create FHIR Observation resources from TSV data and save as JSON files
    """
    logger = logging.getLogger(__name__)
    
    # Read TSV file
    df = pd.read_csv(srcfile, sep='\t', encoding='utf-8', dtype={
        'code': str, 'system': str, 'text_description': str, 
        'value': str, 'units': str, 'ucum': str, 'LowRefRange': str, 'HighRefRange': str, 
        'RR Display': str, 'dateobserved': str
    })
    
    logger.info(f"Processing {len(df)} observations from {srcfile}")
    
    # Load config for subject and performer
    config_path = os.path.join('.', 'config.json')
    with open(config_path) as f:
        config = json.load(f)
    subject_ref = config.get('subject', None)
    performer_val = config.get('performer', None)
    
    # Create observation JSON files
    for index, row in df.iterrows():
        try:
            display = get_concept_display(row['code'], row['system'])
            # Build observation dictionary
            obs_dict = {
                'resourceType': 'Observation',
                'id': str(uuid.uuid4()),
                'status': 'final',
                'category': [
                    {
                        'coding': [
                            {
                                'system': 'http://terminology.hl7.org/CodeSystem/observation-category',
                                'code': 'laboratory',
                                'display': 'Laboratory'
                            }
                        ]
                    }
                ],
                'code': {
                    'coding': [
                        {
                            'system': row['system'],
                            'code': row['code'],
                            'display': display
                        }
                    ],
                    'text': row['text_description'].strip() if pd.notna(row['text_description']) else ""
                },
                'subject': {
                    'reference': subject_ref
                }
            }
            # Add performer
            if performer_val == "unknown":
                obs_dict['performer'] = [
                    {
                        'extension': [
                            {
                                'url': 'http://hl7.org/fhir/StructureDefinition/data-absent-reason',
                                'valueCode': 'unknown'
                            }
                        ]
                    }
                ]
            elif performer_val:
                obs_dict['performer'] = [
                    {
                        'reference': performer_val
                    }
                ]
            
            # If ucum exists use that for unit.code otherwise if a unit exists use that for the unit.code
            ucum_code = row['ucum'].strip() if 'ucum' in row and pd.notna(row['ucum']) and row['ucum'].strip() else (row['units'].strip() if pd.notna(row['units']) else "")
            unit_str = row['units'].strip() if pd.notna(row['units']) else ""
            
            # Set effective date
            if pd.notna(row['dateobserved']) and row['dateobserved']:
                try:
                    # Parse date from MM/DD/YYYY format
                    date_obj = datetime.strptime(row['dateobserved'], '%m/%d/%Y')
                    obs_dict['effectiveDateTime'] = date_obj.date().isoformat()
                except ValueError:
                    logger.warning(f"Invalid date format for row {index}: {row['dateobserved']}")
            
            # Set value quantity, valueString, or valueCodeableConcept
            if pd.notna(row['value']) and row['value']:
                try:
                    value_num = float(row['value'])
                    if value_num > 9:
                        value_num = int(round(value_num))
                    value_quantity: dict[str, int | float] = {}
                    value_metadata: dict[str, str] = {}
                    
                    value_quantity['value'] = value_num
                    if ucum_code:
                        value_metadata['system'] = 'http://unitsofmeasure.org'
                        value_metadata['code'] = ucum_code
                    if unit_str:
                        value_metadata['unit'] = unit_str
                    
                    obs_dict['valueQuantity'] = {**value_quantity, **value_metadata}
                except ValueError:
                    val = row['value'].strip()
                    # Antibiotic sensitivity: S, I, R, etc.
                    abx_map = {
                        'S': {'code': 'S', 'display': 'Susceptible'},
                        'I': {'code': 'I', 'display': 'Intermediate'},
                        'R': {'code': 'R', 'display': 'Resistant'}
                    }
                    if val in abx_map:
                        obs_dict['valueCodeableConcept'] = {
                            'coding': [
                                {
                                    'system': 'http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation',
                                    'code': abx_map[val]['code'],
                                    'display': abx_map[val]['display']
                                }
                            ]
                        }
                    elif val:
                        obs_dict['valueString'] = val
                    else:
                        logger.warning(f"Invalid value for row {index}: {row['value']}")
            
            # Set reference ranges if provided - ranges are inclusive
            # so 10 indicates <= 10.
            if (pd.notna(row['LowRefRange']) and row['LowRefRange']) or \
               (pd.notna(row['HighRefRange']) and row['HighRefRange']):
                
                ref_range = {}
                if pd.notna(row['LowRefRange']) and row['LowRefRange']:
                    try:
                        low_val = float(row['LowRefRange'])
                        low_quantity: dict[str, int | float] = {}
                        low_metadata: dict[str, str] = {}
                        
                        low_quantity['value'] = low_val
                        if ucum_code:
                            low_metadata['system'] = 'http://unitsofmeasure.org'
                            low_metadata['code'] = ucum_code
                        if unit_str:
                            low_metadata['unit'] = unit_str
                        
                        ref_range['low'] = {**low_quantity, **low_metadata}
                    except ValueError:
                        logger.warning(f"Invalid low reference range for row {index}: {row['LowRefRange']}")
                
                if pd.notna(row['HighRefRange']) and row['HighRefRange']:
                    try:
                        high_val = float(row['HighRefRange'])
                        high_quantity: dict[str, int | float] = {}
                        high_metadata: dict[str, str] = {}
                        
                        high_quantity['value'] = high_val
                        if ucum_code:
                            high_metadata['system'] = 'http://unitsofmeasure.org'
                            high_metadata['code'] = ucum_code
                        if unit_str:
                            high_metadata['unit'] = unit_str
                        
                        ref_range['high'] = {**high_quantity, **high_metadata}
                    except ValueError:
                        logger.warning(f"Invalid high reference range for row {index}: {row['HighRefRange']}")
                
                # Add text description for reference range if available
                if pd.notna(row['RR Display']) and row['RR Display']:
                    ref_range['text'] = row['RR Display']
                
                obs_dict['referenceRange'] = [ref_range]
            
            # Create filename using code and index
            safe_code = row['code'].replace('/', '-').replace('\\', '-')
            filename = f"observation_{safe_code}_{index:03d}.json"
            filepath = os.path.join(outdir, filename)
            
            # Save JSON file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(obs_dict, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Created observation file: {filename}")
            
        except Exception as e:
            logger.error(f"Error processing row {index}: {str(e)}")
            continue
   
    # Create a FHIR bundle of observations including "fullUrl"
    bundle_entries = []
    result_history_entries = []
    
    # Re-read the data to create bundle entries
    df = pd.read_csv(srcfile, sep='\t', encoding='utf-8', dtype={
        'code': str, 'system': str, 'text_description': str, 
        'value': str, 'units': str, 'ucum': str, 'LowRefRange': str, 'HighRefRange': str, 
        'RR Display': str, 'dateobserved': str
    })
    
    # Load config for subject and performer
    config_path = os.path.join('.', 'config.json')
    with open(config_path) as f:
        config = json.load(f)
    subject_ref = config.get('subject', None)
    performer_val = config.get('performer', None)
    
    # Create bundle entries for each observation
    for index, row in df.iterrows():
        try:
            display = get_concept_display(row['code'], row['system'])
            obs_id = str(uuid.uuid4())
            
            # Build observation dictionary
            obs_dict = {
                'resourceType': 'Observation',
                'id': obs_id,
                'status': 'final',
                'category': [
                    {
                        'coding': [
                            {
                                'system': 'http://terminology.hl7.org/CodeSystem/observation-category',
                                'code': 'laboratory',
                                'display': 'Laboratory'
                            }
                        ]
                    }
                ],
                'code': {
                    'coding': [
                        {
                            'system': row['system'],
                            'code': row['code'],
                            'display': display
                        }
                    ],
                    'text': row['text_description'].strip() if pd.notna(row['text_description']) else ""
                },
                'subject': {
                    'reference': subject_ref
                }
            }
            
            # Add performer
            if performer_val == "unknown":
                obs_dict['performer'] = [
                    {
                        'extension': [
                            {
                                'url': 'http://hl7.org/fhir/StructureDefinition/data-absent-reason',
                                'valueCode': 'unknown'
                            }
                        ]
                    }
                ]
            elif performer_val:
                obs_dict['performer'] = [
                    {
                        'reference': performer_val
                    }
                ]
            
            # If ucum exists use that for unit.code otherwise if a unit exists use that for the unit.code
            ucum_code = row['ucum'].strip() if 'ucum' in row and pd.notna(row['ucum']) and row['ucum'].strip() else (row['units'].strip() if pd.notna(row['units']) else "")
            unit_str = row['units'].strip() if pd.notna(row['units']) else ""
            
            # Set effective date
            if pd.notna(row['dateobserved']) and row['dateobserved']:
                try:
                    # Parse date from MM/DD/YYYY format
                    date_obj = datetime.strptime(row['dateobserved'], '%m/%d/%Y')
                    obs_dict['effectiveDateTime'] = date_obj.date().isoformat()
                except ValueError:
                    logger.warning(f"Invalid date format for row {index}: {row['dateobserved']}")
            
            # Set value quantity, valueString, or valueCodeableConcept
            if pd.notna(row['value']) and row['value']:
                try:
                    value_num = float(row['value'])
                    if value_num > 9:
                        value_num = int(round(value_num))
                    value_quantity: dict[str, int | float] = {}
                    value_metadata: dict[str, str] = {}
                    
                    value_quantity['value'] = value_num
                    if ucum_code:
                        value_metadata['system'] = 'http://unitsofmeasure.org'
                        value_metadata['code'] = ucum_code
                    if unit_str:
                        value_metadata['unit'] = unit_str
                    
                    obs_dict['valueQuantity'] = {**value_quantity, **value_metadata}
                except ValueError:
                    val = row['value'].strip()
                    # Antibiotic sensitivity: S, I, R, etc.
                    abx_map = {
                        'S': {'code': 'S', 'display': 'Susceptible'},
                        'I': {'code': 'I', 'display': 'Intermediate'},
                        'R': {'code': 'R', 'display': 'Resistant'}
                    }
                    if val in abx_map:
                        obs_dict['valueCodeableConcept'] = {
                            'coding': [
                                {
                                    'system': 'http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation',
                                    'code': abx_map[val]['code'],
                                    'display': abx_map[val]['display']
                                }
                            ]
                        }
                    elif val:
                        obs_dict['valueString'] = val
                    else:
                        logger.warning(f"Invalid value for row {index}: {row['value']}")
            
            # Set reference ranges if provided
            if (pd.notna(row['LowRefRange']) and row['LowRefRange']) or \
               (pd.notna(row['HighRefRange']) and row['HighRefRange']):
                
                ref_range = {}
                if pd.notna(row['LowRefRange']) and row['LowRefRange']:
                    try:
                        low_val = float(row['LowRefRange'])
                        low_quantity: dict[str, int | float] = {}
                        low_metadata: dict[str, str] = {}
                        
                        low_quantity['value'] = low_val
                        if ucum_code:
                            low_metadata['system'] = 'http://unitsofmeasure.org'
                            low_metadata['code'] = ucum_code
                        if unit_str:
                            low_metadata['unit'] = unit_str
                        
                        ref_range['low'] = {**low_quantity, **low_metadata}
                    except ValueError:
                        logger.warning(f"Invalid low reference range for row {index}: {row['LowRefRange']}")
                
                if pd.notna(row['HighRefRange']) and row['HighRefRange']:
                    try:
                        high_val = float(row['HighRefRange'])
                        high_quantity: dict[str, int | float] = {}
                        high_metadata: dict[str, str] = {}
                        
                        high_quantity['value'] = high_val
                        if ucum_code:
                            high_metadata['system'] = 'http://unitsofmeasure.org'
                            high_metadata['code'] = ucum_code
                        if unit_str:
                            high_metadata['unit'] = unit_str
                        
                        ref_range['high'] = {**high_quantity, **high_metadata}
                    except ValueError:
                        logger.warning(f"Invalid high reference range for row {index}: {row['HighRefRange']}")
                
                # Add text description for reference range if available
                if pd.notna(row['RR Display']) and row['RR Display']:
                    ref_range['text'] = row['RR Display']
                
                obs_dict['referenceRange'] = [ref_range]
            
            # Create bundle entry
            bundle_entry = {
                'fullUrl': f'urn:uuid:{obs_id}',
                'resource': obs_dict
            }
            bundle_entries.append(bundle_entry)
            
            # Add to result history entries for the result history section
            result_history_entries.append({
                'reference': f'urn:uuid:{obs_id}'
            })
            
        except Exception as e:
            logger.error(f"Error processing row {index} for bundle: {str(e)}")
            continue
    
    # Create result history section
    # Generate HTML table rows from the data
    html_rows = []
    for index, row in df.iterrows():
        try:
            # Get display name from terminology server
            display = get_concept_display(row['code'], row['system'])
            test_name = display if display else row['text_description']
            
            # Format the value with units
            if pd.notna(row['value']) and row['value']:
                value_str = str(row['value'])
                if pd.notna(row['units']) and row['units']:
                    value_str += f" {row['units']}"
                elif pd.notna(row['ucum']) and row['ucum']:
                    value_str += f" {row['ucum']}"
            else:
                value_str = ""
            
            # Format the date
            date_str = ""
            if pd.notna(row['dateobserved']) and row['dateobserved']:
                try:
                    date_obj = datetime.strptime(row['dateobserved'], '%m/%d/%Y')
                    date_str = date_obj.strftime('%d/%m/%Y')
                except ValueError:
                    date_str = row['dateobserved']
            
            html_rows.append(f"                  <tr>\n                    <td>{test_name}</td>\n                    <td>{value_str}</td>\n                    <td>{date_str}</td>\n                  </tr>")
        except Exception as e:
            logger.warning(f"Error creating result history row for index {index}: {str(e)}")
            continue
    
    # Create the result history section
    result_history_section = {
        'title': 'Result History',
        'code': {
            'coding': [
                {
                    'system': 'http://loinc.org',
                    'code': '30954-2'
                }
            ]
        },
        'text': {
            'status': 'generated',
            'div': f'<div xmlns="http://www.w3.org/1999/xhtml">\\n              <table border="1">\\n                <thead>\\n                  <tr>\\n                    <th>Test Name</th>\\n                    <th>Test Result</th>\\n                    <th>Date</th>\\n                  </tr>\\n                </thead>\\n                <tbody>\\n{"".join(html_rows)}\\n                </tbody>\\n              </table>\\n            </div>'
        },
        'entry': result_history_entries
    }
    
    # Create the FHIR bundle
    bundle = {
        'resourceType': 'Bundle',
        'id': str(uuid.uuid4()),
        'type': 'collection',
        'entry': bundle_entries
    }
    
    # Add result history as a section in the bundle
    if result_history_entries:
        bundle['section'] = [result_history_section]
    
    # Save bundle to file
    bundle_filename = f"observations_bundle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    bundle_filepath = os.path.join(outdir, bundle_filename)
    
    with open(bundle_filepath, 'w', encoding='utf-8') as f:
        json.dump(bundle, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Created FHIR bundle: {bundle_filename} with {len(bundle_entries)} observations")

def main():
    """
    Check terminology using the $validate-code operation on a single fhir IG npm package
    downloded by the getter function from simplifier.net ig registry
    Keyword arguments:
    rootdir -- Root data folder, where the report file goes
    config.json tells the download which package to download from simplifier.net
    and which errors/warnings can be safely ignored or checked manually.    
    """
    
    homedir=os.environ['HOME']
    parser = argparse.ArgumentParser()
    defaultpath=os.path.join(homedir,"data","gen-obs")

    logger = logging.getLogger(__name__)
    parser.add_argument("-r", "--rootdir", help="Root data folder", default=defaultpath)   
    args = parser.parse_args()
    # Create the data path if it doesn't exist
    check_path(args.rootdir)

    # setup output folder for observations   
    outdir = os.path.join(args.rootdir,"out")
    check_path(outdir)

    ## Setup logging
    now = datetime.now() # current date and time
    ts = now.strftime("%Y%m%d-%H%M%S")
    FORMAT='%(asctime)s %(lineno)d : %(message)s'
    logging.basicConfig(format=FORMAT, encoding='utf-8', filename=os.path.join('logs',f'gen-obs-{ts}.log'),level=logging.INFO)
    logger.info('Started')
    
    # Load config for subject, performer, and srcfile
    config_path = os.path.join('.', 'config.json')
    with open(config_path) as f:
        config = json.load(f)
    srcfile_name = config.get('srcfile', 'srcfile.txt')
    srcfile = os.path.join(args.rootdir, srcfile_name)

    create_observations(srcfile, outdir)
   
    logger.info("Finished")

if __name__ == '__main__':
    main()