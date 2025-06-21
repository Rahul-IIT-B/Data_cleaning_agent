import streamlit as st
import pandas as pd
import os
import subprocess
import sys
from pathlib import Path

def run_cli_pipeline(input_path, output_path, log_path):
    project_root = Path(__file__).parent.resolve()
    cli_path = project_root / "cli.py"
    result = subprocess.run([
        sys.executable, str(cli_path),
        '--input', str(input_path),
        '--output', str(output_path),
        '--log', str(log_path)
    ], capture_output=True, text=True, cwd=project_root)
    return result

def read_logs_with_fallback(log_path):
    try:
        with open(log_path, "r", encoding="utf-8") as logf:
            return logf.readlines()
    except UnicodeDecodeError:
        with open(log_path, "r", encoding="latin-1") as logf:
            return logf.readlines()

def main():
    st.title("Mini-Agent DataFix: Clean Your Customer CSVs")
    st.write("Upload a messy customer CSV, clean it with AI agents, and download the results!")
    uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("### Raw Data", df)
        if st.button("Clean Data"): 
            with st.spinner("Running data-fixing pipeline (CLI)..."):
                project_root = Path(__file__).parent.resolve()
                input_path = project_root / "data/temp_input.csv"
                output_path = project_root / "data/temp_output.csv"
                log_path = project_root / "logs/agent_logs.txt"
                df.to_csv(input_path, index=False, encoding="utf-8")
                result = run_cli_pipeline(input_path, output_path, log_path)
                if result.returncode == 0 and output_path.exists():
                    cleaned_df = pd.read_csv(output_path)
                    st.success("Data cleaned!")
                    st.write("### Cleaned Data", cleaned_df)
                    st.download_button(
                        label="Download Cleaned CSV",
                        data=cleaned_df.to_csv(index=False).encode('utf-8'),
                        file_name="cleaned_output.csv",
                        mime="text/csv"
                    )
                    if log_path.exists():
                        logs = read_logs_with_fallback(log_path)
                        with st.expander("Show Agent Logs"):
                            for log in logs:
                                st.text(log.strip())
                else:
                    st.error("Pipeline failed. See details below:")
                    st.text(result.stderr)

if __name__ == "__main__":
    main()
