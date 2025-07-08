import piecash
import re
import yaml
from collections import defaultdict, Counter
import string

# Example mutual_funds mapping with aliases
MUTUAL_FUNDS_CONFIG = {
    "PPFAS": {"mf_number": 64, "aliases": ["ppfas", "parag parikh"]},
    "UTI": {"mf_number": 28, "aliases": ["uti"]},
    "Aditya Birla Sun Life": {"mf_number": 3, "aliases": ["aditya birla sun life", "absl", "birla sun life"]},
    "Franklin Templeton": {"mf_number": 27, "aliases": ["franklin", "franklin templeton"]},
    "HDFC": {"mf_number": 9, "aliases": ["hdfc"]},
    "ICICI Prudential": {"mf_number": 20, "aliases": ["icici", "icici prudential"]},
    "SBI": {"mf_number": 22, "aliases": ["sbi", "state bank of india"]},
}

def clean_and_extract_keywords(description):
    desc = description.lower()
    desc = re.sub(r'\d+', ' ', desc)
    desc = desc.translate(str.maketrans(string.punctuation, ' '*len(string.punctuation)))
    words = desc.split()
    stopwords = set([
        'upi', 'payment', 'transaction', 'bank', 'credit', 'debit', 'auto', 'cc', 'mf', 'bpay', 'bill', 'pay', 'direct',
        'plan', 'growth', 'sip', 'monthly', 'fund', 'mutual', 'deposit', 'recurring', 'transfer', 'online', 'paymentid',
        'id', 'txn', 'ref', 'refid', 'transactionid', 'remarks', 'paid', 'via', 'to', 'from', 'on', 'at', 'the', 'and',
        'for', 'of', 'in', 'a', 'an', 'with', 'by', 'is', 'as', 'or', 'this', 'that', 'it', 'be', 'are', 'was', 'were',
        'has', 'have', 'had', 'but', 'not', 'no', 'yes', 'if', 'else', 'then', 'so', 'do', 'does', 'did', 'can', 'could',
        'would', 'should', 'will', 'shall', 'may', 'might', 'must', 'also', 'just', 'like', 'such', 'some', 'any', 'all',
        'each', 'every', 'other', 'more', 'most', 'many', 'much', 'few', 'several', 'one', 'two', 'three', 'four', 'five',
        'six', 'seven', 'eight', 'nine', 'ten', "sip", "bil", "bpay", "cms"
    ])
    keywords = [w for w in words if w not in stopwords and len(w) > 2]
    return list(sorted(set(keywords)))  # All unique keywords, sorted

def extract_fund_house(account_name, mutual_funds):
    account_name_lower = account_name.lower()
    for house, info in mutual_funds.items():
        for alias in info.get("aliases", []):
            if alias.lower() in account_name_lower:
                return house
    return None

def generate_account_rules(gnucash_file_path, output_yaml_path):
    book = piecash.open_book(gnucash_file_path, open_if_lock=True)
    account_desc_map = defaultdict(list)
    account_amount_date_map = defaultdict(list)
    mf_accounts = set()
    mf_amfi_map = {}
    mf_fund_house_map = {}
    asset_accounts = set()

    for txn in book.transactions:
        desc = txn.description or ''
        txn_date = txn.post_date
        for split in txn.splits:
            acct = split.account
            acct_name = acct.fullname
            amount = float(split.value)
            account_desc_map[acct_name].append(desc)
            account_amount_date_map[acct_name].append((txn_date, amount))
            # Use account.type for classification
            if acct.type == "MUTUAL":
                mf_accounts.add(acct_name)
                mf_amfi_map[acct_name] = getattr(acct.commodity, "mnemonic", "")
                fund_house = extract_fund_house(acct_name, MUTUAL_FUNDS_CONFIG)
                if fund_house:
                    mf_fund_house_map[acct_name] = fund_house
            if acct.type == "ASSET":
                asset_accounts.add(acct_name)

    book.close()
    rules = []
    for acct_name, desc_list in account_desc_map.items():
        # All unique keywords as patterns
        all_keywords = set()
        for d in desc_list:
            all_keywords.update(clean_and_extract_keywords(d))
        freq = Counter(all_keywords)
        top_keywords = [k for k, c in freq.most_common(100)]
        rule = {
            'account': acct_name,
            'patterns': sorted(top_keywords)
        }
        # Add value_conditions for ASSET accounts, using only credit (positive) transactions
        if acct_name in asset_accounts:
            amounts_dates = account_amount_date_map[acct_name]
            # Filter only credit (positive) transactions
            credit_txns = [(dt, amt) for dt, amt in amounts_dates if amt > 0]
            if credit_txns:
                latest_credit = max(credit_txns, key=lambda x: x[0])
                latest_amount_int = int(round(abs(latest_credit[1])))
                rule['value_conditions'] = [{'amount': latest_amount_int}]
        # Add mutual_fund section only for MUTUAL type accounts
        if acct_name in mf_accounts:
            amfi_code = mf_amfi_map.get(acct_name, '')
            fund_house = mf_fund_house_map.get(acct_name)
            rule['mutual_fund'] = {
                'fund_house': fund_house if fund_house else "UNKNOWN",
                'amfi_scheme_code': amfi_code,
                'price_determine': True
            }
        rules.append(rule)

    # Build mutual_funds mapping for YAML, including aliases
    mutual_funds = {
        house: {'mf_number': info['mf_number'], 'aliases': info['aliases']}
        for house, info in MUTUAL_FUNDS_CONFIG.items()
    }

    yaml_data = {
        'mutual_funds': mutual_funds,
        'rules': rules
    }
    with open(output_yaml_path, 'w') as f:
        yaml.dump(yaml_data, f, sort_keys=False)
    print(f"account_rules.yaml generated at {output_yaml_path}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python generate_account_rules.py <input.gnucash> <output.yaml>")
    else:
        generate_account_rules(sys.argv[1], sys.argv[2])

