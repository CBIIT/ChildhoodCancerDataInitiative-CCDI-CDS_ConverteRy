# ChildhoodCancerDataInitiative-CCDI_to_CDS_ConverteRy (ARCHIVED)
This script will take a [CCDI submission template](https://github.com/CBIIT/ccdi-model/tree/main/metadata-manifest) and transform it to a flattened [CDS submission template](https://github.com/CBIIT/cds-model/tree/main/metadata-manifest).

Run the following command in a terminal where python is installed for help.


```
python CCDI-CDS_ConverteRy.py -h
```

```
usage: CCDI-CDS_ConverteRy.py [-h] -f FILENAME -t TEMPLATE

This script will take a CCDI metadata manifest file and converts to a CDS template based on a fixed set of property equivalencies.

required arguments:
  -f FILENAME, --filename FILENAME
                        CCDI dataset file (.xlsx)
  -t TEMPLATE, --template TEMPLATE
                        CDS dataset template file, CDS_submission_metadata_template.xlsx
```
