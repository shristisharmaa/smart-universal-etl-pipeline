🚀 Smart Universal ETL Pipeline
📌 Overview

Smart Universal ETL Pipeline is a Streamlit-based data engineering project that allows users to upload any CSV file, automatically perform data cleaning, transformation, feature engineering, and store the processed data into a SQLite database. It also generates instant insights and visual analytics.

⚙️ Features
📂 Upload any CSV file
🧹 Automatic data cleaning (duplicates & missing values handling)
📅 Auto-detection & conversion of date columns
🔧 Feature engineering (e.g., revenue calculation from price & quantity)
🗄️ Load processed data into SQLite database
📊 Auto-generated insights (summary, statistics, data types)
📈 Visual analytics (category distribution, revenue trends)
⚡ Simple and interactive Streamlit UI
🛠️ Tech Stack
Python 🐍
Streamlit 🎈
Pandas 📊
SQLite 🗄️
📁 Project Structure
smart-universal-etl-pipeline/
│
├── app.py              # Main Streamlit application
├── queries.sql         # SQL queries for analytics
├── smart_etl.db        # SQLite database (auto-generated)
├── requirements.txt    # Dependencies
└── README.md           # Project documentation
🚀 How to Run Locally
# Clone repository
git clone https://github.com/your-username/smart-universal-etl-pipeline.git

# Go to project folder
cd smart-universal-etl-pipeline

# Install dependencies
pip install -r requirements.txt

# Run Streamlit app
streamlit run app.py
📊 Example Use Case
Upload retail CSV data
System automatically cleans and processes it
Calculates total revenue
Stores data in database
Shows insights like:
Dataset shape
Category distribution
Revenue analysis
🎯 Project Goal

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline with automation and analytics, useful for Data Engineering and Data Analyst roles.

👨‍💻 Author

Shristi Sharma

⭐ If you like this project

Give it a ⭐ on GitHub and feel free to fork it!

## 🌐 Live Demo
https://smart-universal-etl-pipeline-shristii.streamlit.app/
