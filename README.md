# FHIR Observation Generator

This tool generates FHIR Observation resources from TSV (Tab-Separated Values) data files.

## Features

- Reads TSV files with laboratory observation data
- Read display for codes from Terminology Server (Ontoserver)
- Generates FHIR R4 compliant Observation resources
- Supports reference ranges (low/high values)
- Handles date parsing in MM/DD/YYYY format
- Creates individual JSON files for each observation
- Comprehensive logging

## Input Data Format

The input TSV file should contain the following columns:

| Column | Description | Required |
|--------|-------------|----------|
| `code` | LOINC or other coding system code | Yes |
| `system` | Coding system URI (e.g., http://loinc.org) | Yes |
| `panel_description` | Human readable context for the individual Observation | No |
| `text_description` | Text description of the observation | Yes |
| `value` | Numeric value of the observation | No |
| `units` | Units of measurement | No |
| `ucum` | UCUM code for the units of measure | Yes | 
| `LowRefRange` | Low reference range value | No |
| `HighRefRange` | High reference range value | No |
| `RR Display` | Reference range display text | No |
| `dateobserved` | Date of observation (DD/MM/YYYY) | No |

## Usage

```bash
python main.py [-r ROOT_DIR]
```

- `-r, --rootdir`: Root data folder (default: `$HOME/data/gen-obs`)

The script expects:
- Input file: `{rootdir}/srcfile.txt` (TSV format)
- Output directory: `{rootdir}/out/` (created automatically)
- Log files: `logs/gen-obs-{timestamp}.log`

## Example Input Data

```tsv
code	system	text_description	value	units	LowRefRange	HighRefRange	RR Display	dateobserved
2085-9	http://loinc.org	HDL Cholesterol	1.2	mmol/L	1.0	2.0	Normal: 1.0-2.0 mmol/L	12/6/2024
2089-1	http://loinc.org	LDL Cholesterol	3.5	mmol/L	0.0	3.0	Optimal: <3.0 mmol/L	12/6/2024
2571-8	http://loinc.org	Triglycerides	1.8	mmol/L	0.0	1.7	Normal: <1.7 mmol/L	12/6/2024
```

## Output

The script generates individual JSON files for each observation:
- Filename format: `observation_{code}_{index}.json`
- FHIR R4 compliant Observation resources
- Each file contains a complete FHIR Observation with:
  - Resource type, ID, and status
  - Laboratory category coding
  - Observation code and display text
  - Value with units (if provided)
  - Reference ranges (if provided)
  - Effective date (if provided)

## Dependencies

- pandas
- fhirclient

## Error Handling

The script includes comprehensive error handling:
- Invalid date formats are logged as warnings
- Invalid numeric values are logged as warnings
- Processing continues even if individual rows fail
- All errors are logged with detailed information
