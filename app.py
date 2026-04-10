import streamlit as st
import pandas as pd
import sqlite3
import logging

# ---------------- LOGGING ----------------
logging.basicConfig(
    filename="etl_logs.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)

def log(msg):
    logging.info(msg)
    st.write(f"📌 {msg}")


# ---------------- ETL FUNCTIONS ----------------

def extract(file):
    df = pd.read_csv(file)
    log("Data extracted successfully")
    return df


def transform(df):
    log("Starting transformation")

    df = df.drop_duplicates()
    df = df.fillna(0)

    # date detection
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # feature engineering
    if "price" in df.columns and "quantity" in df.columns:
        df["total_revenue"] = df["price"] * df["quantity"]
        log("Revenue column created")

    log("Transformation completed")
    return df


def load(df):
    conn = sqlite3.connect("smart_etl.db")
    df.to_sql("data", conn, if_exists="replace", index=False)
    log("Data loaded into SQLite (table: data)")
    return conn


def run_query(query):
    conn = sqlite3.connect("smart_etl.db")
    result = pd.read_sql_query(query, conn)
    return result


def get_tables():
    conn = sqlite3.connect("smart_etl.db")
    tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table';",
        conn
    )
    return tables


# ---------------- STREAMLIT UI ----------------

st.set_page_config(page_title="DE Level ETL Pipeline", layout="wide")
st.title("Smart Universal ETL Pipeline")

# ---------------- FILE UPLOAD ----------------

file = st.file_uploader("Upload CSV File", type="csv")

if file:

    # EXTRACT
    df = extract(file)
    st.subheader("📌 Raw Data")
    st.dataframe(df)

    # TRANSFORM
    df = transform(df)
    st.subheader("📌 Transformed Data")
    st.dataframe(df)

    # LOAD
    conn = load(df)

    st.success("ETL Completed Successfully 🚀")

    # ---------------- DATABASE INFO ----------------
    st.markdown("---")
    st.subheader("🗄 Database Info")

    st.info("👉 All SQL queries run on table: data")

    

    # ---------------- SQL ENGINE ----------------
    st.markdown("---")
    st.subheader("🧠 SQL Query Engine")

    query = st.text_area("Write SQL Query (on table: data)")

    if st.button("Run SQL Query"):
        try:
            result = run_query(query)
            st.dataframe(result)
            log("SQL query executed successfully")
        except Exception as e:
            st.error(f"Error: {e}")
            log(f"SQL error: {e}")