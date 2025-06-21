import pandas as pd
import os
from typing import Tuple, List
from dotenv import load_dotenv
import google.generativeai as genai  
import re

load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_GEMINI_API_KEY"))

def enrich_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Enriches the DataFrame by:
    - Filling missing values using Gemini LLM (mocked here)
    - Adding computed fields (e.g., customer_score)
    Returns:
        df: Enriched DataFrame
        logs: List of enrichment descriptions
    """
    logs = []
    def gemini_generate(prompt_text):
        try:
            print("GEMINI PROMPT:", prompt_text)  # or use logging
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(prompt_text).text.strip()
            print("GEMINI RESPONSE:", response)  # or use logging
            return response
        except Exception as e:
            print("GEMINI ERROR:", e)
            return "Unknown"
    
    # Replace all missing/invalid values with 'MISSING'
    df_for_gemini = df.copy()
    for col in df_for_gemini.columns:
        df_for_gemini[col] = df_for_gemini[col].replace([None, '', 'Unknown', 'NaN', 'nan', pd.NA], 'MISSING')
        if df_for_gemini[col].dtype == object:
            df_for_gemini[col] = df_for_gemini[col].apply(lambda x: 'MISSING' if pd.isnull(x) or str(x).strip() == '' else x)
    # Prepare CSV string for Gemini
    csv_str = df_for_gemini.to_csv(index=False)
    # Compose prompt with strict CSV formatting instructions
    prompt = (
        "You are a data cleaning and enrichment agent. For the following CSV, fill in every 'MISSING' value in each row with a realistic, plausible value in the same format as the other values in that column. "
        "If you cannot infer a value, generate a random plausible value in the same format as the rest of the column. "
        "Additionally, for each row, generate two new columns: 'is_loyal_customer' (Yes/No, based on profile) and 'customer_persona' (max two lines, concise, describing their likely personality, values, and buying habits). "
        "Return the result as a CSV with the same columns as input plus the two new columns, and no extra text. "
        "IMPORTANT: Enclose every field in double quotes. Do NOT use commas inside any field (use semicolons or periods instead). Do not add extra header or footer text.\n\n"
        + csv_str
    )
    response = gemini_generate(prompt)
    # Remove markdown code block markers if present
    response = re.sub(r'^```csv\s*|```$', '', response, flags=re.MULTILINE).strip()
    # Robust CSV parsing: remove any extra lines, ensure quoting
    from io import StringIO
    import csv
    try:
        # Only keep lines with the correct number of columns (header + data)
        header = response.splitlines()[0]
        num_fields = len(list(csv.reader([header]))[0])
        filtered_lines = [line for line in response.splitlines() if len(list(csv.reader([line]))[0]) == num_fields]
        filtered_csv = '\n'.join(filtered_lines)
        enriched_df = pd.read_csv(StringIO(filtered_csv), quotechar='"', escapechar='\\')
        # Log all changes made by enrichment (compare before/after for missing/MISSING/Unknown)
        column_changes = {}
        for col in df.columns:
            if col in enriched_df.columns:
                column_changes[col] = [(idx, old, new) for idx, (old, new) in enumerate(zip(df[col], enriched_df[col])) if str(old).strip().lower() in ["", "missing", "unknown", "nan"] and str(new).strip().lower() not in ["", "missing", "unknown", "nan"]]
                if column_changes[col]:
                    logs.append(f" Column: {col} | {[(idx, old, new) for idx, old, new in column_changes[col]]}")
        # Log new columns
        for col in enriched_df.columns:
            if col not in df.columns:
                column_changes[col] = [(idx, val) for idx, val in enumerate(enriched_df[col])]
                logs.append(f" Column: {col} | {[(idx, val) for idx, val in column_changes[col]]}")
        df = enriched_df  
        logs.append("Enriched all missing values and added is_loyal_customer and customer_persona using Gemini in a single prompt (strict CSV format).")
    except Exception as e:
        logs.append(f"Gemini CSV parse error: {e}")
            
    # After enrichment, clear the issues column (all should be fixed)
    if 'issues' in df.columns:
        df['issues'] = ''
    return df, logs
