# Mini Agent-Based Data Fixing System

## Overview

This project implements a modular, agent-based pipeline for cleaning and enriching messy customer CSV data. The system is designed to be robust, extensible, and easy to use, supporting both command-line and web interfaces. Each agent is responsible for a specific stage in the data cleaning process, and all actions are logged in a clear, human-readable format.

---

## Architecture

### Agents

1. **Detection Agent** (`agents/detection_agent.py`)

   - Scans the input CSV for common data issues:
     - Missing or malformed emails, phone numbers, gender, marital status, age, loyalty points, country, names
     - Duplicate rows
     - Non-canonical or misspelled country/city names
   - Logs all detected issues in a compact, grouped format
   - Only creates an `issues` column if issues are found

2. **Correction Agent** (`agents/correction_agent.py`)

   - Fixes issues flagged by the detection agent:
     - Standardizes names, gender, marital status, phone numbers
     - Removes duplicates
     - Corrects country and city names using fuzzy matching against canonical lists
     - Fixes implausible ages and loyalty points
   - Logs all corrections per column, grouped as (row, old, new) tuples

3. **Enrichment Agent** (`agents/enrichment_agent.py`)

   - Fills missing values using Gemini LLM
   - Adds new columns (e.g., `is_loyal_customer`, `customer_persona`)
   - Logs all enrichment actions per column

4. **Pipeline/Orchestration** (`cli.py`)

   - Runs the agents in sequence: Detection → Correction → Enrichment
   - Iterates up to 3 times or until all issues are resolved
   - Logs all actions in a unified, readable log file

5. **Web UI** (`webapp.py`)
   - Streamlit app for uploading, cleaning, and downloading CSVs
   - Displays logs and results interactively

---

## How It Works

1. **Input:**

   - Give a messy customer CSV either by uploading it to the streamlit webapp or directly placing in the `data/` directory (e.g., `sample_input.csv`) and then using CLI to run.
   - The sample file includes a variety of realistic errors and edge cases.

2. **Pipeline:**

   - The CLI or webapp loads the CSV and runs the detection agent.
   - If issues are found, the correction agent fixes them.
   - If missing values remain, the enrichment agent fills them and adds new attributes.
   - The process repeats up to 3 times or until no issues remain.
   - All actions are logged in `logs/agent_logs.txt`.

3. **Output:**
   - Cleaned CSV is saved to the specified output path.
   - Logs are available for review.

---

## How to Run & Test


### Requirements

- Python 3.8+
- Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```
- For enrichment, set up Gemini API credentials as per `.env` instructions.
   ```
   GOOGLE_GEMINI_API_KEY = YOUR_GEMINI_API_KEY
   ```

### Command-Line Interface

Run the pipeline on a CSV:

```bash
python cli.py --input data/sample_input.csv --output data/cleaned_output.csv --log logs/agent_logs.txt
```
Replace sample_input with the messy file name 

- The cleaned CSV and logs will be saved in the specified locations.

### Web Interface

Start the Streamlit app:

```bash
streamlit run webapp.py
```

- Upload a CSV, clean it, and download the results interactively.

---

## File Structure

```
Data_Cleaning_Agent/
├── agents/
│   ├── detection_agent.py
│   ├── correction_agent.py
│   ├── enrichment_agent.py
│   └── city_list.txt (not required, cities now in code)
├── data/
│   ├── sample_input.csv
│   └── cleaned_output.csv
├── logs/
│   └── agent_logs.txt
├── cli.py
├── webapp.py
├── requirements.txt
└── README.md
```
---

## Notes

- All agents are modular and can be run/tested independently.
- Logs are grouped by column and action for easy review.
- The system is robust to malformed data and logs all actions for transparency.

---
