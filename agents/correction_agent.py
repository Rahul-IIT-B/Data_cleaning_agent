import pandas as pd
from rapidfuzz import process, fuzz
from typing import Tuple, List

# Expanded canonical country list
CANONICAL_COUNTRIES = [
    'United States', 'India', 'Canada', 'United Kingdom', 'Australia', 'Germany', 'France', 'Japan', 'China', 'Brazil', 'South Korea',
    'Italy', 'Spain', 'Mexico', 'Russia', 'Netherlands', 'Sweden', 'Norway', 'Denmark', 'Finland', 'Switzerland', 'Austria',
    'Belgium', 'Ireland', 'New Zealand', 'Singapore', 'Malaysia', 'Thailand', 'Indonesia', 'Turkey', 'Saudi Arabia', 'UAE',
    'South Africa', 'Egypt', 'Argentina', 'Chile', 'Colombia', 'Peru', 'Poland', 'Portugal', 'Greece', 'Czech Republic',
    'Hungary', 'Romania', 'Slovakia', 'Slovenia', 'Croatia', 'Estonia', 'Latvia', 'Lithuania', 'Philippines', 'Vietnam',
    'Pakistan', 'Bangladesh', 'Sri Lanka', 'Nepal', 'Israel', 'Qatar', 'Kuwait', 'Oman', 'Morocco', 'Kenya', 'Nigeria',
    'Ghana', 'Venezuela', 'Ecuador', 'Uruguay', 'Paraguay', 'Bolivia', 'Costa Rica', 'Panama', 'Guatemala', 'Honduras',
    'El Salvador', 'Dominican Republic', 'Cuba', 'Jamaica', 'Trinidad and Tobago', 'Iceland', 'Luxembourg', 'Liechtenstein',
    'Monaco', 'Andorra', 'San Marino', 'Malta', 'Cyprus', 'Bahrain', 'Jordan', 'Lebanon', 'Syria', 'Iraq', 'Iran', 'Afghanistan',
    'Uzbekistan', 'Kazakhstan', 'Azerbaijan', 'Georgia', 'Armenia', 'Mongolia', 'Cambodia', 'Laos', 'Myanmar', 'Brunei', 'Timor-Leste'
]
# Canonical city list 
CANONICAL_CITIES = [
    'Springfield', 'Seattle', 'Miami Beach', 'San Francisco', 'Bludhaven', 'Hub City', 'Metropolis', 'Opal City', 'Gateway City',
    'Houston', 'Central City', 'Bellevue', 'Riverside', 'New York', 'Gotham', 'Coast City', 'Los Angeles', 'Star City', 'Miami',
    'National City', 'Mumbai', 'Newark', 'Dallas', 'Unknown'
]
GENDERS = ['Male', 'Female', 'Other']
MARITAL_STATUSES = ['Single', 'Married', 'Divorced', 'Widowed']

def correct_issues(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Applies corrections to the DataFrame for all columns.
    Returns:
        df: Cleaned DataFrame
        logs: List of correction descriptions
    """
    logs = []
    column_changes = {}
    # Standardize name columns (title case)
    for col in ['first_name', 'last_name', 'full_name']:
        if col in df.columns:
            before = df[col].copy()
            df[col] = df[col].astype(str).str.title().str.strip()
            df[col] = df[col].replace(['Nan', 'nan', '', None], 'Unknown')
            column_changes[col] = [(idx, old, new) for idx, (old, new) in enumerate(zip(before, df[col])) if old != new]
            if column_changes[col]:
                logs.append(f" Standardized {col} | {[(idx, old, new) for idx, old, new in column_changes[col]]}")

    # Remove duplicates (all columns)
    before = len(df)
    df = df.drop_duplicates(keep='first').copy()
    after = len(df)
    if before != after:
        logs.append(f" Removed {before - after} duplicate rows.")

    # Normalize country names (fuzzy match, only if not canonical)
    if 'country' in df.columns:
        def match_country(val):
            val_clean = str(val).strip().title()
            if pd.isnull(val) or not val_clean or val_clean == 'Unknown':
                return 'Unknown'
            if val_clean in CANONICAL_COUNTRIES:
                return val_clean
            match, score, _ = process.extractOne(val_clean, CANONICAL_COUNTRIES, scorer=fuzz.token_sort_ratio)
            return match if score > 70 else 'Unknown'
        before = df['country'].copy()
        df['country'] = df['country'].apply(match_country)
        column_changes['country'] = [(idx, old, new) for idx, (old, new) in enumerate(zip(before, df['country'])) if old != new]
        if column_changes['country']:
            logs.append(f" Standardized country using fuzzy matching | {[(idx, old, new) for idx, old, new in column_changes['country']]}")

    # Set negative/zero/implausible ages to median positive age
    if 'age' in df.columns:
        median_age = df[df['age'].apply(lambda x: isinstance(x, (int, float)) and 0 < x < 120)]['age'].median()
        before = df['age'].copy()
        df['age'] = df['age'].apply(lambda x: median_age if pd.isnull(x) or not isinstance(x, (int, float)) or x <= 0 or x > 120 else x)
        column_changes['age'] = [(idx, old, new) for idx, (old, new) in enumerate(zip(before, df['age'])) if old != new]
        if column_changes['age']:
            logs.append(f" Standardized age | {[(idx, old, new) for idx, old, new in column_changes['age']]}")

    # Set negative loyalty points to 0
    if 'loyalty_points' in df.columns:
        before = df['loyalty_points'].copy()
        df['loyalty_points'] = df['loyalty_points'].apply(lambda x: 0 if pd.isnull(x) or not isinstance(x, (int, float)) or x < 0 else x)
        column_changes['loyalty_points'] = [(idx, old, new) for idx, (old, new) in enumerate(zip(before, df['loyalty_points'])) if old != new]
        if column_changes['loyalty_points']:
            logs.append(f" Standardized loyalty_points | {[(idx, old, new) for idx, old, new in column_changes['loyalty_points']]}")

    # Standardize gender
    if 'gender' in df.columns:
        before = df['gender'].copy()
        df['gender'] = df['gender'].apply(lambda x: x if x in GENDERS else 'Other')
        column_changes['gender'] = [(idx, old, new) for idx, (old, new) in enumerate(zip(before, df['gender'])) if old != new]
        if column_changes['gender']:
            logs.append(f" Standardized gender | {[(idx, old, new) for idx, old, new in column_changes['gender']]}")

    # Standardize marital status
    if 'marital_status' in df.columns:
        before = df['marital_status'].copy()
        df['marital_status'] = df['marital_status'].apply(lambda x: x if x in MARITAL_STATUSES else 'Single')
        column_changes['marital_status'] = [(idx, old, new) for idx, (old, new) in enumerate(zip(before, df['marital_status'])) if old != new]
        if column_changes['marital_status']:
            logs.append(f" Standardized marital_status | {[(idx, old, new) for idx, old, new in column_changes['marital_status']]}")

    # Standardize phone (keep only digits and format)
    if 'phone' in df.columns:
        before = df['phone'].copy()
        df['phone'] = df['phone'].astype(str).str.replace(r'[^\d]', '', regex=True).str.pad(8, fillchar='0')
        df['phone'] = df['phone'].apply(lambda x: f"{x[-8:-4]}-{x[-4:]}" if len(x) >= 8 else '0000-0000')
        column_changes['phone'] = [(idx, old, new) for idx, (old, new) in enumerate(zip(before, df['phone'])) if old != new]
        if column_changes['phone']:
            logs.append(f" Standardized phone | {[(idx, old, new) for idx, old, new in column_changes['phone']]}")

    # Fuzzy match and standardize city names (only if not canonical)
    if 'city' in df.columns:
        def match_city(val):
            val_clean = str(val).strip().title()
            if pd.isnull(val) or not val_clean or val_clean == 'Unknown':
                return 'Unknown'
            if val_clean in CANONICAL_CITIES:
                return val_clean
            match, score, _ = process.extractOne(val_clean, CANONICAL_CITIES, scorer=fuzz.token_sort_ratio)
            return match if score > 40 else 'Unknown'
        before = df['city'].copy()
        df['city'] = df['city'].apply(match_city)
        column_changes['city'] = [(idx, old, new) for idx, (old, new) in enumerate(zip(before, df['city'])) if old != new]
        if column_changes['city']:
            logs.append(f" Standardized city using fuzzy matching | {[(idx, old, new) for idx, old, new in column_changes['city']]}")

    # Remove the issues column if present 
    if 'issues' in df.columns:
        df = df.drop(columns=['issues'])
    return df, logs
