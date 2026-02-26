import os
import re
import pandas as pd
import duckdb
from openai import AzureOpenAI
from dotenv import load_dotenv
from dev import maintain_tokens_count
# =========================================================
# CONFIG SECTION  (Later → config.py)
# =========================================================

load_dotenv()

deployment = "gpt-4.1"
api_key = os.getenv("AZURE_OPENAI_API_KEY")
azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT_TEXT")
api_version = "2024-02-15-preview"

DB_PATH = "temp_data.duckdb"
TABLE_NAME = "uploaded_data"

if not api_key or not azure_endpoint:
    raise RuntimeError("Azure OpenAI configuration is missing.")

client = AzureOpenAI(
    api_version=api_version,
    azure_endpoint=azure_endpoint,
    api_key=api_key,
)

# =========================================================
# FILE LOADING SECTION  (Later → data_loader.py)
# =========================================================

def load_file(file_path):
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    elif file_path.endswith(".xlsx"):
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file format")
    return df


# =========================================================
# DATA CLEANING SECTION  (Later → preprocessing.py)
# =========================================================

def clean_data(df):
    df.columns = df.columns.str.strip()
    df.replace("", pd.NA, inplace=True)

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("Unknown")
        else:
            df[col] = df[col].fillna(df[col].median())

    return df


# =========================================================
# METADATA SECTION  (Later → metadata.py)
# =========================================================

def extract_dataframe_metadata(df):
    return {
        "columns": list(df.columns),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "sample_rows": df.head(5).to_dict(orient="records")
    }


def extract_metadata_from_db(table_name=TABLE_NAME):
    con = duckdb.connect(DB_PATH)

    schema = con.execute(f"DESCRIBE {table_name}").fetchdf()
    sample = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()

    con.close()

    metadata = {
        "columns": schema["column_name"].tolist(),
        "dtypes": dict(zip(schema["column_name"], schema["column_type"])),
        "sample_rows": sample.to_dict(orient="records")
    }

    return metadata


# =========================================================
# LLM UTILITIES SECTION  (Later → llm_utils.py)
# =========================================================

def strip_sql_markdown(text: str) -> str:
    pattern = r"```(?:\w+)?\s*(.*?)```"
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1).strip()
    return text.strip()


# =========================================================
# TABLE CREATION SECTION  (Later → schema_generator.py)
# =========================================================

def generate_create_table_sql(metadata, table_name=TABLE_NAME):
    system_prompt = f"""
You are a SQL expert.
Generate a DuckDB CREATE TABLE statement.

Table Name: {table_name}

Columns: {metadata['columns']}
Types: {metadata['dtypes']}
Sample Rows: {metadata['sample_rows']}

Return ONLY valid SQL.
"""

    response = client.chat.completions.create(
        model=deployment,
        messages=[{"role": "system", "content": system_prompt}],
        temperature=0
    )
    maintain_tokens_count(response)
    print("[CREATE TABLE LLM CALL]")

    return strip_sql_markdown(response.choices[0].message.content.strip())


# =========================================================
# DATABASE SECTION  (Later → db_manager.py)
# =========================================================

def store_table(df, create_sql, table_name=TABLE_NAME):
    con = duckdb.connect(DB_PATH)

    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(create_sql)

    con.register("temp_df", df)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")

    con.close()


def verify_table(table_name=TABLE_NAME):
    con = duckdb.connect(DB_PATH)

    print("\n--- Verification (First 20 Rows) ---\n")
    result = con.execute(
        f"SELECT * FROM {table_name} LIMIT 20;"
    ).fetchdf()

    print(result)

    con.close()


def execute_query(query):
    con = duckdb.connect(DB_PATH)
    result = con.execute(query).fetchdf()
    con.close()
    return result


# =========================================================
# ANALYSIS SECTION  (Later → query_engine.py)
# =========================================================

def is_safe_query(query: str) -> bool:
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE"]
    upper_query = query.upper()
    return not any(keyword in upper_query for keyword in forbidden)


def enforce_limit(query: str, limit=10):
    if "LIMIT" not in query.upper():
        query = query.rstrip(";") + f" LIMIT {limit};"
    return query


def generate_select_sql(user_query, metadata, table_name=TABLE_NAME):
    system_prompt = f"""
You are a DuckDB SQL expert.

You must generate ONLY a SELECT query.
Never modify data.
Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE.

Before Generating SQL query you should consder all columns and sample rows to understand user's intent
to generate proper sql query , because sometimes column names and record inside it can be ambigious so
you must torelarate that edge case okay 

Table Name: {table_name}
Columns: {metadata['columns']}
Types: {metadata['dtypes']}
Sample Rows : {metadata['sample_rows']}

Return ONLY SQL.
"""

    response = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ],
        temperature=0
    )
    maintain_tokens_count(response)

    print("[ANALYSIS LLM CALL]")

    return strip_sql_markdown(response.choices[0].message.content.strip())


# =========================================================
# REPORT GENERATION SECTION  (Later → reporting.py)
# =========================================================

def generate_summary(user_query, result_df):
    if result_df.empty:
        return "No records matched the query."

    preview = result_df.head(10).to_dict(orient="records")

    system_prompt = """
You are a data analyst.
Write a concise factual summary of the query result.
Do not hallucinate.
Only use provided data.
"""

    response = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"""
User Question:
{user_query}

Query Result:
{preview}
"""}
        ],
        temperature=0.2
    )

    print("[SUMMARY LLM CALL]")
    maintain_tokens_count(response)

    return response.choices[0].message.content.strip()


# =========================================================
# MAIN PIPELINE SECTION  (Later → main.py)
# =========================================================

def process_file(file_path):
    print("Loading file...")
    df = load_file(file_path)

    print("Cleaning data...")
    df = clean_data(df)

    print("Extracting metadata...")
    metadata = extract_dataframe_metadata(df)

    print("Generating CREATE TABLE SQL...")
    create_sql = generate_create_table_sql(metadata)

    print("\nGenerated SQL:\n")
    print(create_sql)

    print("\nStoring table...")
    store_table(df, create_sql)

    verify_table()

    print("\nInitial pipeline completed successfully.\n")


if __name__ == "__main__":

    file_path =  "testdata.csv" #"sample_SKUs.xlsx" # change as needed
    process_file(file_path)

    # Extract metadata ONCE for interactive analysis
    metadata = extract_metadata_from_db()

    while True:
        user_input = input("\nAsk something (or type exit): ")

        if user_input.lower() == "exit":
            break

        sql_query = generate_select_sql(user_input, metadata)

        if not is_safe_query(sql_query):
            print("Unsafe query blocked.")
            continue

        sql_query = enforce_limit(sql_query)

        print("\nGenerated SQL:\n", sql_query)

        result_df = execute_query(sql_query)

        print("\n--- Result (First 10 Rows) ---\n")
        print(result_df.head(10))

        summary = generate_summary(user_input, result_df)

        print("\n--- Summary ---\n")
        print(summary)
