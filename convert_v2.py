from mf_nav_util import get_nav_for_date
import pandas as pd
import re
import yaml
import sys
import os

from datetime import datetime, timedelta


## TODO: First match the accounts with value_conditions and then match without them.
## TODO: if date is a holiday, go to next business day
## TODO: if there are multiple matches, prompt user to choose an account (may be with interactive flag on, else don't assign any accounts)



def load_rules(config_file="account_rules.yaml"):
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    return config['rules'], config.get('mutual_funds', {})

def determine(description, value, rules):
    description_lower = description.lower()
    for rule in rules:
        for pattern in rule.get('patterns', []):
            if re.search(pattern, description_lower, re.IGNORECASE):
                value_conditions = rule.get('value_conditions', [])
                if value_conditions:
                    for cond in value_conditions:
                        if 'amount' in cond and abs(cond['amount'] - value) < 0.01:
                            return rule
                    continue
                else:
                    return rule
    return None

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 convert_gnucash.py <bank_statement.csv> <account_rules.yaml>")
        sys.exit(1)

    input_file = sys.argv[1]
    if not os.path.isfile(input_file):
        print(f"Error: File '{input_file}' does not exist.")
        sys.exit(1)

    rules_file = sys.argv[2]
    if not os.path.isfile(rules_file):
        print(f"Error: File '{rules_file}' does not exist.")
        sys.exit(1)

    rules, mutual_funds = load_rules(rules_file)

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
            'Full Account Name': 'Assets:Current Assets: Savings - ICICI',
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
                fund_house = rule['mutual_fund']['fund_house']
                amfi_scheme_code = rule['mutual_fund']['amfi_scheme_code']
                if fund_house not in mutual_funds:
                    print(f"Warning: Fund house '{fund_house}' not found in mutual_funds mapping.")
                    mf_number = None
                else:
                    mf_number = mutual_funds[fund_house]['mf_number']
                if mf_number:
                    nav_price = get_nav_for_date(mf_number, amfi_scheme_code, date)
                else:
                    nav_price = None
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

