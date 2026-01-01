import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR
PROFILING_DIR = BASE_DIR / "profiling"
OUTPUT_DIR = BASE_DIR / "output"

PROFILING_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

def load_csv(name):
    print(f"Loading {name}")
    return pd.read_csv(DATA_DIR / name)

user_referrals = load_csv("user_referrals.csv")
user_referral_logs = load_csv("user_referral_logs.csv")
user_logs = load_csv("user_logs.csv")
paid_transactions = load_csv("paid_transactions.csv")
referral_rewards = load_csv("referral_rewards.csv")
user_referral_statuses = load_csv("user_referral_statuses.csv")
lead_logs = load_csv("lead_log.csv")

print("✅ All CSV files loaded\n")

profiling = []

def profile(df, table):
    for c in df.columns:
        profiling.append({
            "table": table,
            "column": c,
            "dtype": str(df[c].dtype),
            "null_count": int(df[c].isna().sum()),
            "distinct_count": int(df[c].nunique())
        })

profile(user_referrals, "user_referrals")
profile(user_referral_logs, "user_referral_logs")
profile(user_logs, "user_logs")
profile(paid_transactions, "paid_transactions")
profile(referral_rewards, "referral_rewards")
profile(user_referral_statuses, "user_referral_statuses")
profile(lead_logs, "lead_log")

pd.DataFrame(profiling).to_excel(
    PROFILING_DIR / "profiling_report.xlsx", index=False
)
print("📊 Profiling report generated")

def to_datetime(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)

to_datetime(user_referrals, ["referral_at", "updated_at"])
to_datetime(user_referral_logs, ["created_at"])
to_datetime(user_logs, ["membership_expired_date"])
to_datetime(paid_transactions, ["transaction_at"])
to_datetime(referral_rewards, ["created_at"])
to_datetime(user_referral_statuses, ["created_at"])
to_datetime(lead_logs, ["created_at"])

user_referrals.dropna(subset=["referral_id", "referrer_id"], inplace=True)
user_logs.dropna(subset=["user_id"], inplace=True)

user_logs["is_deleted"] = user_logs["is_deleted"].fillna(False).astype(bool)

def initcap(df, exclude=None):
    exclude = exclude or []
    for c in df.select_dtypes(include="object").columns:
        if c not in exclude:
            df[c] = df[c].astype(str).str.strip().str.title()

initcap(user_referrals)
initcap(user_referral_logs)
initcap(user_logs, exclude=["homeclub"])
initcap(paid_transactions)
initcap(referral_rewards)
initcap(user_referral_statuses)
initcap(lead_logs)

print("🧹 Data cleaning done")

df = user_referrals.merge(
    user_referral_statuses,
    left_on="user_referral_status_id",
    right_on="id",
    how="left"
)

df = df.merge(
    referral_rewards,
    left_on="referral_reward_id",
    right_on="id",
    how="left",
    suffixes=("", "_reward")
)

df = df.merge(
    user_logs.add_prefix("referrer_"),
    left_on="referrer_id",
    right_on="referrer_user_id",
    how="left"
)

df = df.merge(
    paid_transactions,
    on="transaction_id",
    how="left"
)

df = df.merge(
    user_referral_logs,
    left_on="referral_id",
    right_on="user_referral_id",
    how="left"
)

df = df.merge(
    lead_logs,
    left_on="referee_id",
    right_on="lead_id",
    how="left"
)

df.drop_duplicates(subset=["referral_id"], inplace=True)
print("🔗 All joins completed")

df["reward_value"] = (
    df["reward_value"]
    .astype(str)
    .str.replace(",", "", regex=False)
)
df["reward_value"] = pd.to_numeric(df["reward_value"], errors="coerce")

df["transaction_status"] = df["transaction_status"].astype(str).str.upper()
df["transaction_type"] = df["transaction_type"].astype(str).str.upper()

print("🔢 Data type fixes done")

def tz_naive(s):
    if s is not None and hasattr(s.dtype, "tz"):
        return s.dt.tz_convert(None)
    return s

for c in ["transaction_at", "referral_at", "referrer_membership_expired_date"]:
    if c in df.columns:
        df[c] = tz_naive(df[c])

print("⏱️ Timezone normalization done")

def source_category(row):
    if row["referral_source"] == "User Sign Up":
        return "Online"
    if row["referral_source"] == "Draft Transaction":
        return "Offline"
    if row["referral_source"] == "Lead":
        return row["source_category"]
    return None

df["referral_source_category"] = df.apply(source_category, axis=1)

df["is_business_logic_valid"] = False

valid_success = (
    (df["reward_value"].notna()) &
    (df["reward_value"] > 0) &
    (df["description"] == "Berhasil") &
    (df["transaction_status"] == "PAID") &
    (df["transaction_type"] == "NEW") &
    (df["transaction_at"] > df["referral_at"]) &
    (df["transaction_at"].dt.to_period("M") == df["referral_at"].dt.to_period("M")) &
    (df["referrer_is_deleted"] == False) &
    (df["referrer_membership_expired_date"] >= df["transaction_at"]) &
    (df["is_reward_granted"] == True)
)

valid_no_reward = (
    (df["description"].isin(["Menunggu", "Tidak Berhasil"])) &
    (df["reward_value"].isna())
)

df.loc[valid_success | valid_no_reward, "is_business_logic_valid"] = True
print("✅ Business logic applied")

final_columns = [
    "referral_id",
    "referral_source",
    "referral_source_category",
    "referral_at",
    "referrer_user_id",
    "referrer_name",
    "referrer_phone_number",
    "referrer_homeclub",
    "referee_id",
    "referee_name",
    "referee_phone",
    "description",
    "reward_value",
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "created_at",
    "is_business_logic_valid"
]

final_df = df[final_columns]

final_df.to_csv(
    OUTPUT_DIR / "referral_reward_validation.csv",
    index=False
)

print("🎯 FINAL REPORT GENERATED")
print("📁 output/referral_reward_validation.csv")
print(f"📊 Total rows: {len(final_df)}")
