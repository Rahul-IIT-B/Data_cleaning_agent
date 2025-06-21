import pandas as pd
from typing import Tuple, List

EMAIL_REGEX = r"^[\w\.-]+@[\w\.-]+\.\w+$"
PHONE_REGEX = r"^\d{3,4}-\d{4}$"
GENDERS = ['Male', 'Female', 'Other']
MARITAL_STATUSES = ['Single', 'Married', 'Divorced', 'Widowed']

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

def detect_issues(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Scans the DataFrame for common data issues in all columns.
    Returns:
        df: DataFrame with an 'issues' column
        logs: List of issue descriptions
    """
    issues = []
    logs = []
    column_changes = {}
    # Email
    if 'email' in df.columns:
        invalid_email_mask = ~df['email'].astype(str).str.match(EMAIL_REGEX)
        missing_email_mask = df['email'].isnull() | (df['email'].astype(str).str.strip() == '')
        email_issues_mask = invalid_email_mask | missing_email_mask
        issues.append(email_issues_mask.map(lambda x: 'Invalid Email' if x else ''))
        column_changes['email'] = [(idx, val) for idx, val in df[email_issues_mask].email.items()]
        logs.append(f" {email_issues_mask.sum()} missing or malformed emails | {[(idx, val) for idx, val in column_changes['email']]}")
    # Phone
    if 'phone' in df.columns:
        invalid_phone_mask = ~df['phone'].astype(str).str.match(PHONE_REGEX)
        issues.append(invalid_phone_mask.map(lambda x: 'Invalid Phone' if x else ''))
        column_changes['phone'] = [(idx, val) for idx, val in df[invalid_phone_mask].phone.items()]
        logs.append(f" {invalid_phone_mask.sum()} invalid phone numbers | {[(idx, val) for idx, val in column_changes['phone']]}")
    # Gender
    if 'gender' in df.columns:
        invalid_gender_mask = ~df['gender'].isin(GENDERS)
        issues.append(invalid_gender_mask.map(lambda x: 'Invalid Gender' if x else ''))
        column_changes['gender'] = [(idx, val) for idx, val in df[invalid_gender_mask].gender.items()]
        logs.append(f" {invalid_gender_mask.sum()} invalid gender | {[(idx, val) for idx, val in column_changes['gender']]}")
    # Marital Status
    if 'marital_status' in df.columns:
        invalid_marital_mask = ~df['marital_status'].isin(MARITAL_STATUSES)
        issues.append(invalid_marital_mask.map(lambda x: 'Invalid Marital Status' if x else ''))
        column_changes['marital_status'] = [(idx, val) for idx, val in df[invalid_marital_mask].marital_status.items()]
        logs.append(f" {invalid_marital_mask.sum()} invalid marital status | {[(idx, val) for idx, val in column_changes['marital_status']]}")
    # Age
    if 'age' in df.columns:
        invalid_age_mask = df['age'].apply(lambda x: pd.isnull(x) or (isinstance(x, (int, float)) and (x <= 0 or x > 120)))
        issues.append(invalid_age_mask.map(lambda x: 'Invalid Age' if x else ''))
        column_changes['age'] = [(idx, val) for idx, val in df[invalid_age_mask].age.items()]
        logs.append(f" {invalid_age_mask.sum()} missing, negative, or implausible ages | {[(idx, val) for idx, val in column_changes['age']]}")
    # Loyalty Points
    if 'loyalty_points' in df.columns:
        invalid_loyalty_mask = df['loyalty_points'].apply(lambda x: pd.isnull(x) or (isinstance(x, (int, float)) and x < 0))
        issues.append(invalid_loyalty_mask.map(lambda x: 'Invalid Loyalty Points' if x else ''))
        column_changes['loyalty_points'] = [(idx, val) for idx, val in df[invalid_loyalty_mask].loyalty_points.items()]
        logs.append(f" {invalid_loyalty_mask.sum()} invalid loyalty points | {[(idx, val) for idx, val in column_changes['loyalty_points']]}")
    # Country
    if 'country' in df.columns:
        non_canonical_mask = ~df['country'].isin(CANONICAL_COUNTRIES)
        issues.append(non_canonical_mask.map(lambda x: 'Non-canonical Country' if x else ''))
        column_changes['country'] = [(idx, val) for idx, val in df[non_canonical_mask].country.items()]
        logs.append(f" {non_canonical_mask.sum()} non-canonical or misspelled countries | {[(idx, val) for idx, val in column_changes['country']]}")
    # Duplicates
    duplicate_mask = df.duplicated(keep=False)
    issues.append(duplicate_mask.map(lambda x: 'Duplicate' if x else ''))
    column_changes['duplicate'] = [(idx, 'DUPLICATE') for idx in df[duplicate_mask].index]
    logs.append(f" {duplicate_mask.sum()} duplicate rows found. | {[(idx) for idx, val in column_changes['duplicate']]}")
    # Empty names
    for col in ['first_name', 'last_name', 'full_name']:
        if col in df.columns:
            empty_name_mask = df[col].isnull() | (df[col].astype(str).str.strip() == '') | (df[col].astype(str).str.lower() == 'nan')
            issues.append(empty_name_mask.map(lambda x: f'Empty {col}' if x else ''))
            column_changes[col] = [(idx, val) for idx, val in df[empty_name_mask][col].items()]
            logs.append(f" {empty_name_mask.sum()} empty {col} | {[(idx, val) for idx, val in column_changes[col]]}")
    # Combine all issues into a single column
    def combine_issues(row):
        return ', '.join([str(issue) for issue in row if issue])
    if issues:
        issues_df = pd.DataFrame(issues).T
        df['issues'] = issues_df.apply(combine_issues, axis=1)
    elif 'issues' in df.columns:
        df = df.drop(columns=['issues'])
    return df, logs
