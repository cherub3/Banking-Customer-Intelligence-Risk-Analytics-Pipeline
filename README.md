# 💳 Banking Customer Intelligence & Risk Analytics Pipeline

## 📌 Project Overview
This project builds an **end-to-end data analytics pipeline** for banking transactions to generate **customer intelligence, risk monitoring, and business insights**.

The system processes **6.3M+ transactions** and **2.3M+ customers**, transforming raw data into actionable insights using **SQL + Python**.

---

## 🎯 Objectives
- Build a **data quality pipeline** to clean and validate financial transactions  
- Design a **scalable data model** (customers, accounts, transactions)  
- Perform **feature engineering (RFM + risk metrics)**  
- Generate **customer segmentation and risk profiling**  
- Deliver **business insights for decision-making**  

---

## 🛠️ Tech Stack
- **SQL (MySQL 8+)** → Data cleaning, modeling, analytics  
- **Python (pandas, matplotlib)** → EDA & analysis  
- **Jupyter Notebook** → Exploratory Data Analysis  
- **Power BI (conceptual)** → Dashboard design  

---

## 📂 Project Structure

├── load_raw_data.sql
├── stage1_cleaning.sql
├── stage2_modeling.sql
├── stage3_features.sql
├── stage4_analytics.sql
├── eda.ipynb
├── optional_pandas_pipeline.py
├── business_metrics_output.txt
└── README.md


---

## 📊 Dataset
- Source: PaySim Synthetic Financial Dataset  
- Size: **6,362,620 transactions**  
- Customers: **~2.37 million**  

👉 Dataset Link:  
https://www.kaggle.com/datasets/ealaxi/paysim1

---

## 🔄 Pipeline Overview

### Stage 1 — Data Quality & Cleaning
- Removed duplicates (full-row deduplication)
- Validated financial constraints (balance consistency)
- Segregated invalid records into reject table

**Outputs:**
- `staging_transactions`
- `staging_transactions_rejects`

---

### Stage 2 — Data Modeling
- Built **dimensional model**:
  - `dim_customer`
  - `dim_account`
  - `fact_transaction`
- Created deterministic IDs using hashing

---

### Stage 3 — Feature Engineering
- RFM metrics:
  - Recency
  - Frequency
  - Monetary
- Risk indicators:
  - Fraud rate
  - High-value transaction ratio
  - Merchant exposure
  - Velocity (7d vs 30d)

---

### Stage 4 — Analytics & Insights
- Pareto analysis (80/20 rule)
- Monthly transaction trends
- Segment-wise revenue contribution
- Risk scoring & watchlist
- Dormant customer analysis

---

## 📈 Key Business Insights

- **Revenue Concentration:**  
  Top **21.39% customers contribute 80% of total value**

- **Dormant Customers:**  
  **98.5% customers inactive**, representing **₹74.8B value**

- **Fraud Analysis:**  
  Fraud rate is **0.33%**, but concentrated in high-risk segments

- **Risk Segmentation:**  
  High-risk customers = **5.65%**

- **Customer Base Imbalance:**  
  Majority customers fall into **DORMANT segment**

---

## 📊 Exploratory Data Analysis (EDA)
The notebook (`eda.ipynb`) includes:
- Transaction distributions  
- Customer behavior analysis  
- Segment-level insights  
- Correlation analysis  
- Business-focused observations  

---

## 📌 Business Impact

- Identified **high-value customer concentration risk**
- Highlighted **reactivation opportunities for dormant users**
- Built **risk monitoring framework for fraud detection**
- Enabled **data-driven segmentation for targeted strategies**

---

## 📊 Dashboard Design (Conceptual)

### Executive Summary
- Total transactions, value, fraud rate  
- Monthly trends  

### Customer Segmentation
- Segment distribution  
- Revenue contribution  

### Risk Monitoring
- High-risk customer watchlist  
- Fraud patterns  

---

## 🚀 How to Run

1. Load dataset into MySQL:
   - Run `load_raw_data.sql`

2. Execute pipeline:
   - `stage1_cleaning.sql`
   - `stage2_modeling.sql`
   - `stage3_features.sql`
   - `stage4_analytics.sql`

3. Run EDA:
   - Open `eda.ipynb`

---

## 🔑 Skills Demonstrated
- Data Cleaning & Validation  
- SQL (Advanced Queries, Window Functions)  
- Data Modeling (Star Schema)  
- Feature Engineering  
- Exploratory Data Analysis  
- Business Insight Generation  

---

## 📌 Future Improvements
- Add predictive modeling (fraud/churn prediction)  
- Integrate dashboard (Power BI/Tableau)  
- Automate pipeline (Airflow/dbt)  

---

## 👤 Author
Final-year Computer Science student focused on **data analytics & business intelligence roles**.
