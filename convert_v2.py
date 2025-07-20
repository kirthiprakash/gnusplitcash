import uuid
from mf_nav_util import get_nav_for_date
import pandas as pd
import re
import yaml
import sys
import os

## TODO: First match the accounts with value_conditions and then match without them.
## TODO: if there are multiple matches, prompt user to choose an account (may be with interactive flag on, else don't assign any accounts)


def load_rules(config_file="account_rules.yaml"):
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    return config['rules'], config.get('mutual_funds', {})

import re

def is_all_keywords_present(description, keywords):
    """Returns True if ALL keywords (case-insensitive) appear in description."""
    description_lower = description.lower()
    return all(keyword.lower() in description_lower for keyword in keywords)

def determine(description, value, rules):
    description_lower = description.lower()

    # 1st pass: rules with value_conditions
    for rule in rules:
        value_conditions = rule.get('value_conditions', [])
        if not value_conditions:
            continue
        for pattern in rule.get('patterns', []):
            # Support for multi-keyword patterns as a list
            if isinstance(pattern, list):
                if is_all_keywords_present(description, pattern):
                    for cond in value_conditions:
                        if 'amount' in cond and abs(cond['amount'] - value) < 0.01:
                            return rule
            else:
                if re.search(pattern, description_lower, re.IGNORECASE):
                    for cond in value_conditions:
                        if 'amount' in cond and abs(cond['amount'] - value) < 0.01:
                            return rule

    # 2nd pass: rules without value_conditions
    for rule in rules:
        if rule.get('value_conditions'):
            continue
        for pattern in rule.get('patterns', []):
            if isinstance(pattern, list):
                if is_all_keywords_present(description, pattern):
                    return rule
            else:
                if re.search(pattern, description_lower, re.IGNORECASE):
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

        transaction_id = str(uuid.uuid4())  

        # First split (bank account)
        split1 = {
            'TransactionID': transaction_id,
            'date': date,
            'description': description,
            'Full Account Name': 'Assets:Current Assets:Savings - ICICI',
            'Amount': -value if withdrawal > 0 else value,
            'Value': value,
            'price': ''
        }

        # optional third split for mutual fund purchase stamp duty
        stamp_duty_split = None

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
                    # deduct stamp duty of 0.05% on MF purchase
                    stamp_duty = value * (0.005 / 100)
                    value_minus_duty= value - stamp_duty
                    amount2 = value_minus_duty / nav_price
                    value = value - stamp_duty

                    stamp_duty_split = {
                    'TransactionID': transaction_id,
                    'date': date,
                    'description': description,
                    'Full Account Name': 'Expenses:Taxes:Mutual Fund Purchase Stamp Duty',
                    'Amount': stamp_duty,
                    'Value': stamp_duty,
                    'price': ''
                }
        else:
            account2 = "Imbalance-INR"
            price = ''
            amount2 = -split1['Amount']

        split2 = {
            'TransactionID': transaction_id,
            'date': date,
            'description': description,
            'Full Account Name': account2,
            'Amount': amount2,
            'Value': value,
            'price': price
        }

        multi_split_transactions.append(split1)
        multi_split_transactions.append(split2)
        if stamp_duty_split:
            multi_split_transactions.append(stamp_duty_split)

    # Convert to DataFrame and export as CSV
    multi_split_df = pd.DataFrame(multi_split_transactions)
    output_file = "multi_split_gnucash.csv"
    multi_split_df.to_csv(output_file, index=False)
    print(f"Conversion complete. Output written to {output_file}")

if __name__ == "__main__":
    main()

