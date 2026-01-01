# Springer Referral Reward Validation Pipeline

## Overview
This project implements a data profiling and validation pipeline for a user referral program.
The objective is to determine whether referral rewards are valid or invalid based on defined
business rules and to identify potential fraud scenarios.

The pipeline processes multiple data sources, applies data cleaning and transformations,
and generates a final report indicating reward validity for each referral.

---

## Tech Stack
- Python 3.9
- Pandas
- Docker

---

## Project Structure
├── src/
│ └── referral_pipeline.py
├── output/
│ └── referral_reward_validation.csv
├── profiling/
│ └── profiling_report.xlsx
├── data_dictionary.xlsx
├── requirements.txt
├── Dockerfile
└── README.md

---

## Data Sources
The pipeline uses the following input tables:
- user_referrals
- user_referral_logs
- user_logs
- user_referral_statuses
- referral_rewards
- paid_transactions
- lead_log

Each table is profiled to capture null counts and distinct values.

---

## Business Logic (Summary)
A referral reward is considered **valid** if:
- Reward value is greater than 0
- Referral status is **Berhasil (Successful)**
- Transaction is **PAID** and of type **NEW**
- Transaction happens after referral creation and in the same month
- Referrer account is active and membership is not expired
- Reward has been granted

Referrals with status **Menunggu (Pending)** or **Tidak Berhasil (Failed)** are considered valid
only if no reward value is assigned.

All other cases are marked as invalid.

---

## How to Run (Without Docker)
```bash
pip install -r requirements.txt
python src/referral_pipeline.py

---
## **requirements.txt**

pandas
openpyxl
python-dateutil
pytz
---
git add README.md Dockerfile requirements.txt
git commit -m "Add README, Dockerfile and requirements"
git push
---

