import os
from pathlib import Path
import ast
import pandas as pd
import re
import shutil

# set up directories
input_files = Path("files/raw")
files_processed = Path("files/processed")
bad_outputs = Path("files/bad")
metadata = Path("files/meta")
REQUIRED_FIELDS = ['name', 'phone', 'location']
#REQUIRED_FIELDS = ['name', 'phone', 'location','rate','rest_type','dish_liked','reviews_list','cuisines']

#copy csv files to our files/raw directory
src_dir = '.'
dst_dir = 'files/raw'
extension = '.csv'  # or '.txt', '.json', etc.

# Ensure the destination directory exists
os.makedirs(dst_dir, exist_ok=True)

# Copy files with the specified extension
for filename in os.listdir(src_dir):
    if filename.endswith(extension):
        src_path = os.path.join(src_dir, filename)
        dst_path = os.path.join(dst_dir, filename)
        shutil.copy2(src_path, dst_path)

# ensure output directories exist 
for directory in [files_processed, bad_outputs, metadata]:
    directory.mkdir(parents=True, exist_ok=True)

# M1: File Check (file extension is csv and it is not empty)
def is_valid_file(filepath):
    return filepath.suffix == ".csv" and filepath.stat().st_size > 0
# is this a new file?
def get_new_files(folder, processed_files):
    files = folder.glob("*.csv")
    return [f for f in files if f.name not in processed_files and is_valid_file(f)]

# M2: Data Quality Check 
def clean_phone(phone):
    phone = re.sub(r"[^\d]", '', str(phone))
#    if phone == "":
#        print("Phone is an empty string")
#    elif not phone:
#        print("Value is an empty string")
#    else:
#        print(phone)
    return phone

## Validate rules and clean dataframes

def validate_and_clean(df):
    bad_rows = []
    meta_info = []

#### new checking
# is it a comma list separated field? 
    def is_comma_list(val):
      if isinstance(val, str):
        return ',' in val
      return False
# is it a list?
    def is_list(val):
        try:
          parsed = isinstance(val, str) and val.strip().startswith('[') and val.strip().endswith(']')
          #parsed = ast.literal_eval(val) if isinstance(val, str) else val
          return isinstance(parsed, list)
        except:
          return False
###
    # Clean phone numbers to have only numbers
    df['phone'] = df['phone'].apply(clean_phone)

#    Remove special characters in address and reviews_list
    rem_special_char_fields = ['address','reviews_list']
    for field in rem_special_char_fields:
        df[field] = df[field].astype(str).str.replace(r'[^a-zA-Z0-9\s,]', '', regex=True).str.strip()
#
# rules for each column 
#
    df['rate'] = df['rate'].apply(lambda x: str(x) if pd.notnull(x) else x)
    df['rest_type'] = df['rest_type'].apply(lambda x: str(x) if pd.notnull(x) else x)
    df['dish_liked'] = df['dish_liked'].apply(lambda x: is_comma_list(x) if pd.isnull(x) else x)
    df['reviews_list'] = df['reviews_list'].apply(is_list)

    # Drop rows with nulls in required fields
    for field in REQUIRED_FIELDS:
        #taking care of empty or null fields
        nulls = df[df[field].isnull() | (df[field] == '') | (df[field] == False) | (df[field] == None) | (df[field] == '[]') ]
        if not nulls.empty:
            #keeping track of rows that have wrong or missing information
            bad_rows.append(nulls)
            meta_info.append({"type": f"null_in_{field}", "rows": list(nulls.index)})
    #remove trailing and leading spaces from phone field
    df['phone'] = df['phone'].astype(str).str.lstrip()
    df['phone'] = df['phone'].astype(str).str.strip()
    df['phone'] = df['phone'].apply(lambda x: int(str(x).strip()) if str(x).strip() != '' else None)
    # Check for duplicates based on name, phone, location
    #duplicates = df[df.duplicated(subset=['name', 'phone', 'location'], keep='first')]
    duplicates = df[df['phone'].notnull() & 
    (df['phone'].astype(str).str.strip() != '') & 
    (df['phone'].astype(str).str.strip() != None) & 
    df.duplicated(subset=['name', 'phone', 'location'], keep='first')]
#    duplicates = df[df.duplicated(subset=['name', 'phone', 'location'], keep='first')]
    if not duplicates.empty:
        #adding bad rows where there are dupplicates
        bad_rows.append(duplicates)
        meta_info.append({"type": "duplicate", "rows": list(duplicates.index)})

    # Remove bad rows from main df
    bad_indices = set(i for b in bad_rows for i in b.index)
    good_df = df.drop(index=bad_indices)
    bad_df = pd.concat(bad_rows).drop_duplicates() if bad_rows else pd.DataFrame()
    #return clean dataframe, bad dataframe info and the metadata
    return good_df, bad_df, meta_info

#  Pipeline Runner 
def process_pipeline():
    processed_files = set(f.name for f in files_processed.glob("*.out"))
    new_files = get_new_files(input_files, processed_files)

    for file in new_files:
        df = pd.read_csv(file)
        clean_df, bad_df, meta = validate_and_clean(df)
        #save them as out files
        clean_df.to_csv(files_processed / f"{file.stem}.out", index=False)

        if not bad_df.empty:
            bad_df.to_csv(bad_outputs / f"{file.stem}.bad", index=False)
            pd.DataFrame(meta).to_json(metadata / f"{file.stem}_meta.json", orient="records", indent=2)

        print(f"Processed: {file.name}")

if __name__ == "__main__":
    process_pipeline()
