from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO

# Year-agnostic fixed-date holidays as (month, day) tuples
FIXED_HOLIDAYS = [
    (1, 26),   # Republic Day
    (8, 15),   # Independence Day
    (10, 2),   # Gandhi Jayanti
    # Add other fixed-date holidays here
]

# Year-specific holidays dictionary: year -> set of holiday dates
YEAR_SPECIFIC_HOLIDAYS = {
    2024: {
        datetime(2024, 3, 25).date(),   # Example variable holiday for 2024
        datetime(2024, 11, 1).date(),   # Another example for 2024
    },
    2025: {
        datetime(2025, 4, 10).date(),
        datetime(2025, 4, 14).date(),
        datetime(2025, 4, 18).date(),   
        datetime(2025, 5, 1).date(),
        datetime(2025, 8, 15).date(),
        datetime(2025, 8, 27).date(),
        datetime(2025, 10, 2).date(),
        datetime(2025, 10, 21).date(),
        datetime(2025, 10, 22).date(),
        datetime(2025, 11, 5).date(),
        datetime(2025, 12, 25).date(),
    }
    # Add more years as needed
}

def is_holiday(date, fixed_holidays=None, year_specific_holidays=None):
    if fixed_holidays is None:
        fixed_holidays = []
    if year_specific_holidays is None:
        year_specific_holidays = set()

    if (date.month, date.day) in fixed_holidays:
        return True
    if date in year_specific_holidays:
        return True
    return False

def get_next_business_day(date, fixed_holidays=None, year_specific_holidays=None):
    if fixed_holidays is None:
        fixed_holidays = []
    if year_specific_holidays is None:
        year_specific_holidays = set()

    next_date = date
    while next_date.weekday() >= 5 or is_holiday(next_date, fixed_holidays, year_specific_holidays):
        next_date += timedelta(days=1)
    return next_date

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

# In-memory NAV cache: {(mf_number, from_date, to_date): DataFrame}
nav_cache = {}

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
    print("fetched MF data for ", cache_key)
    return nav_df


def get_nav_for_date(mf_number, scheme_code, date_str):
    # date_str is expected in 'dd/mm/yyyy'
    date = datetime.strptime(date_str, '%d/%m/%Y').date()

    # Extract year, get year-specific holidays for that year (empty set if none)
    year_holidays = YEAR_SPECIFIC_HOLIDAYS.get(date.year, set())

    # Move to next business day if weekend or holiday
    date = get_next_business_day(date, FIXED_HOLIDAYS, year_holidays)

    from_date = date.strftime('%d-%b-%Y')
    nav_df = fetch_nav_data(mf_number, from_date, from_date)
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
