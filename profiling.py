import pandas as pd
from ydata_profiling import ProfileReport
import ast
import sys

def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"✅ Loaded dataset: {df.shape[0]} rows, {df.shape[1]} columns.")
        return df
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        sys.exit(1)

def validate_fields(df):
    results = {}

    def is_comma_list(val):
        return isinstance(val, str) and ',' in val

    def is_list(val):
        try:
            parsed = ast.literal_eval(val) if isinstance(val, str) else val
            return isinstance(parsed, list)
        except:
            return False

    results['url'] = df['url'].apply(lambda x: isinstance(x, str)).all()
    results['address'] = df['address'].apply(lambda x: isinstance(x, str)).all()
    results['name'] = df['name'].notnull().all() and df['name'].apply(lambda x: isinstance(x, str)).all()
    results['rate'] = df['rate'].apply(lambda x: isinstance(x, str)).all()
    results['votes'] = pd.to_numeric(df['votes'], errors='coerce').notnull().all()
    results['phone'] = df['phone'].notnull().all() and pd.to_numeric(df['phone'], errors='coerce').notnull().all()
    results['location'] = df['location'].notnull().all() and df['location'].apply(lambda x: isinstance(x, str)).all()
    results['rest_type'] = df['rest_type'].apply(lambda x: isinstance(x, str)).all()
    results['dish_liked'] = df['dish_liked'].apply(lambda x: is_comma_list(x) or pd.isnull(x)).all()
    results['cuisines'] = df['cuisines'].apply(lambda x: is_comma_list(x) or pd.isnull(x)).all()
    results['reviews_list'] = df['reviews_list'].apply(lambda x: is_list(x) or pd.isnull(x)).all()

    print("\n📋 Field Validation Results:")
    for field, passed in results.items():
        print(f" - {field:<12}: {'✅ PASS' if passed else '❌ FAIL'}")

    return results
def generate_profile(df, output_path="report.html"):
    print("\n📊 Generating profiling report...")
    profile = ProfileReport(df, title="Zomato Dataset Profile", explorative=True)
    profile.to_file(output_path)
    print(f"✅ Report saved to: {output_path}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Validate and profile Zomato dataset.")
    parser.add_argument("file", help="Path to the CSV file")
    parser.add_argument("--output", default="report.html", help="Path to save the profile report")
    args = parser.parse_args()

    df = load_data(args.file)
    validate_fields(df)
    generate_profile(df, args.output)

if __name__ == "__main__":
    main()
