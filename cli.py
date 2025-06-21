import pandas as pd
import click
import logging
import os
import traceback
from agents.detection_agent import detect_issues
from agents.correction_agent import correct_issues
from agents.enrichment_agent import enrich_data

def setup_logging(log_path):
    logging.basicConfig(
        filename=log_path,
        format="%(asctime)s — %(levelname)s — %(message)s",
        level=logging.INFO,
        filemode='a'
    )

def needs_correction(df):
    # Correction is needed if any issues are present
    # Ensure 'issues' column is always string type to avoid .str accessor errors
    if 'issues' in df.columns:
        return df['issues'].astype(str).str.strip().any()
    return False

def needs_enrichment(df):
    # Check for missing values or 'MISSING' or 'Unknown' in any cell (excluding 'issues' column)
    for col in df.columns:
        if col == 'issues':
            continue
        if df[col].isnull().any() or (df[col].astype(str).str.strip().str.lower().isin(['missing', 'unknown', 'nan', ''])).any():
            return True
    return False

@click.command()
@click.option('--input', '-i', required=True, help='Path to input CSV file')
@click.option('--output', '-o', required=True, help='Path to output cleaned CSV file')
@click.option('--log', '-l', default='logs/agent_logs.txt', help='Path to log file')
def main(input, output, log):
    os.makedirs(os.path.dirname(log), exist_ok=True)
    setup_logging(log)
    try:
        df = pd.read_csv(input)
        logging.info(f"Loaded input CSV: {input}")
        max_iterations = 3
        iteration = 0
        while iteration < max_iterations:
            iteration += 1
            logging.info(f"--- Pipeline Iteration {iteration} ---")
            # Detection (always at start of loop)
            df, detect_logs = detect_issues(df)
            for entry in detect_logs:
                logging.info(f"[Detection Agent] {entry}")
            # Correction if needed
            if needs_correction(df):
                logging.info(f"Correction needed, forwarding to Correction Agent.")
                df, correction_logs = correct_issues(df)
                for entry in correction_logs:
                    logging.info(f"[Correction Agent] {entry}")
                # Enrichment if needed after correction
                if needs_enrichment(df):
                    logging.info(f"Enrichment needed after correction, forwarding to Enrichment Agent.")
                    df, enrich_logs = enrich_data(df)
                    for entry in enrich_logs:
                        logging.info(f"[Enrichment Agent] {entry}")
                    logging.info(f"Forwarding to Detection Agent for re-check.")
                    if iteration == max_iterations:
                        df, detect_logs = detect_issues(df)
                        for entry in detect_logs:
                            logging.info(f"[Detection Agent] {entry}")
                        if not df['issues'].str.strip().any():
                            logging.info(f"All issues resolved after {iteration} iterations.")
                        else:
                            logging.warning(f"Maximum iterations ({max_iterations}) reached. Some issues may remain.")
                            remaining_issues = df[df['issues'].str.strip() != '']
                            if not remaining_issues.empty:
                                logging.warning(f"Rows with unresolved issues after max iterations:")
                                for idx, row in remaining_issues.iterrows():
                                    logging.warning(f"Row {idx}: {row['issues']}")
                            else:
                                logging.info(f"All issues resolved after {iteration} iterations.")
                else:
                    logging.info(f"No enrichment needed after correction.")
                    break
            else:
                logging.info(f"No correction needed after detection.")
                break
            
        df.to_csv(output, index=False)
        logging.info(f"Saved cleaned CSV: {output}")
        click.echo(f"Pipeline completed. Cleaned data saved to {output}. Logs at {log}.")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}\n{traceback.format_exc()}")
        click.echo(f"Error: {e}", err=True)
        exit(1)

if __name__ == '__main__':
    main()
