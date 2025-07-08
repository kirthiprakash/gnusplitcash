import pandas as pd
import re
import yaml
import sys
import os
import requests
from io import StringIO
from datetime import datetime, timedelta

# In-memory NAV cache: {(mf_number, from_date, to_date): DataFrame}
nav_cache = {}

def load_rules(config_file="account_rules_auto_generated.yaml"):
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    return config['rules'], config.get('mutual_funds', {})

def parse_amfi_nav_data(data):
    lines = data.splitlines()
    # Find the header line
    header_line = None
    for i, line in enumerate(lines):
        if line.startswith('Scheme Code;'):
            header_line = i
            break
    if header_line is None:
        raise ValueError("Header line not found in AMFI NAV data")
    # Extract lines from header onwards
    valid_lines = lines[header_line:]
    # Filter out blank and malformed lines
    filtered_lines = []
    for line in valid_lines:
        if line.strip() == '':
            continue
        parts = line.split(';')
        if len(parts) == 8:
            filtered_lines.append(line)
    if not filtered_lines:
        raise ValueError("No valid NAV data lines found")
    # Read into DataFrame
    filtered_data = '\n'.join(filtered_lines)
    df = pd.read_csv(StringIO(filtered_data), sep=';')
    return df

def fetch_nav_data(mf_number, from_date, to_date):
    cache_key = (mf_number, from_date, to_date)
    if cache_key in nav_cache:
        return nav_cache[cache_key]
    url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={mf_number}&tp=1&frmdt={from_date}&todt={to_date}"
    response = requests.get(url)
    response.raise_for_status()
    nav_df = parse_amfi_nav_data(response.text)
    # Clean up date column
    nav_df['Date'] = pd.to_datetime(nav_df['Date'], format='%d-%b-%Y', errors='coerce')
    nav_cache[cache_key] = nav_df
    return nav_df

def get_nav_for_date(mf_number, scheme_code, date_str):
    # date_str is expected in 'dd/mm/yyyy'
    date = datetime.strptime(date_str, '%d/%m/%Y').date()
    from_date = (date - timedelta(days=7)).strftime('%d-%b-%Y')
    to_date = (date + timedelta(days=7)).strftime('%d-%b-%Y')
    nav_df = fetch_nav_data(mf_number, from_date, to_date)
    # Ensure Scheme Code is str for comparison
    nav_df['Scheme Code'] = nav_df['Scheme Code'].astype(str)
    scheme_code = str(scheme_code)
    filtered = nav_df[
        (nav_df['Scheme Code'] == scheme_code) &
        (nav_df['Date'] == pd.Timestamp(date))
    ]
    if filtered.empty:
        filtered = nav_df[
            (nav_df['Scheme Code'] == scheme_code) &
            (nav_df['Date'] <= pd.Timestamp(date))
        ].sort_values('Date', ascending=False).head(1)
    if filtered.empty:
        return None
    try:
        return float(filtered.iloc[0]['Net Asset Value'])
    except Exception:
        return None

def determine(description, value, rules):
    description_lower = description.lower()
    for rule in rules:
        for pattern in rule.get('patterns', []):
            if re.search(pattern, description_lower, re.IGNORECASE):
                value_conditions = rule.get('value_conditions', [])
                if value_conditions:
                    for cond in value_conditions:
                        if 'amount' in cond and cond['amount'] == value:
                            return rule
                    continue
                else:
                    return rule
    return None

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 convert_gnucash.py <bank_statement.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    if not os.path.isfile(input_file):
        print(f"Error: File '{input_file}' does not exist.")
        sys.exit(1)

    rules, mutual_funds = load_rules("account_rules.yaml")

    # Read the bank statement CSV file
    bank_df = pd.read_csv(input_file)

    # Prepare list to hold multi-split transactions
    multi_split_transactions = []

    for idx, row in bank_df.iterrows():
        date = row['Value Date']
        description = str(row['Transaction Remarks'])
        withdrawal = float(row['Withdrawal Amount (INR )'])
        deposit = float(row['Deposit Amount (INR )'])
        value = withdrawal if withdrawal > 0 else deposit

        # First split (bank account)
        split1 = {
            'date': date,
            'description': description,
            'Full Account Name': 'Assets:Current Assets: Savings',
            'Amount': -value if withdrawal > 0 else value,
            'Value': value,
            'price': ''
        }

        # Second split (counter account)
        rule = determine(description, value, rules)
        if rule:
            account2 = rule['account']
            price = ''
            amount2 = -split1['Amount']
            if 'mutual_fund' in rule and rule['mutual_fund'].get('price_determine', False):
                fund_name = rule['mutual_fund']['fund_house']
                amfi_scheme_code = rule['mutual_fund']['amfi_scheme_code']
                mf_number = mutual_funds[fund_name]['mf_number']
                nav_price = get_nav_for_date(mf_number, amfi_scheme_code, date)
                if nav_price:
                    price = nav_price
                    amount2 = value / nav_price
        else:
            account2 = "Expenses:Unknown"
            price = ''
            amount2 = -split1['Amount']

        split2 = {
            'date': date,
            'description': description,
            'Full Account Name': account2,
            'Amount': amount2,
            'Value': value,
            'price': price
        }

        multi_split_transactions.append(split1)
        multi_split_transactions.append(split2)

    # Convert to DataFrame and export as CSV
    multi_split_df = pd.DataFrame(multi_split_transactions)
    output_file = "multi_split_gnucash.csv"
    multi_split_df.to_csv(output_file, index=False)
    print(f"Conversion complete. Output written to {output_file}")

if __name__ == "__main__":
    main()

