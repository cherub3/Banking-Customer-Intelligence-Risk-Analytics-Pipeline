import hashlib
import numpy as np
import pandas as pd


def source_hash_row(row: pd.Series) -> str:
    vals = [
        row["step"], row["type"], row["amount"], row["nameOrig"], row["oldbalanceOrg"],
        row["newbalanceOrig"], row["nameDest"], row["oldbalanceDest"], row["newbalanceDest"],
        row["isFraud"], row["isFlaggedFraud"]
    ]
    key = "|".join(map(lambda x: "" if pd.isna(x) else str(x), vals))
    return hashlib.sha256(key.encode("utf-8")).hexdigest().upper()


def det_id(prefix: str, value: str, n: int = 16) -> str:
    return f"{prefix}_{hashlib.sha256(value.encode('utf-8')).hexdigest().upper()[:n]}"


def qscore(series: pd.Series, reverse: bool = False) -> pd.Series:
    ranked = series.rank(method="first")
    bins = pd.qcut(ranked, 5, labels=[1, 2, 3, 4, 5]).astype(int)
    return (6 - bins) if reverse else bins


def main():
    # Stage 0: load
    raw = pd.read_csv("paysim.csv")

    # Stage 1: cleaning + staging
    df = raw.drop_duplicates().copy()
    df["type"] = df["type"].astype(str).str.strip().str.upper()
    df["nameOrig"] = df["nameOrig"].astype(str).str.strip()
    df["nameDest"] = df["nameDest"].astype(str).str.strip()

    numeric_cols = [
        "step", "amount", "oldbalanceOrg", "newbalanceOrig",
        "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["isFlaggedFraud"] = df["isFlaggedFraud"].fillna(0)

    valid_types = {"PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"}
    cond_missing = (
        df[["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud"]].isna().any(axis=1)
        | df["nameOrig"].eq("")
        | df["nameDest"].eq("")
    )
    cond_amount = df["amount"] <= 0
    cond_neg_bal = (df[["oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"]] < 0).any(axis=1)
    cond_same_party = df["nameOrig"].eq(df["nameDest"])
    cond_type = ~df["type"].isin(valid_types)

    df["reject_reason"] = np.select(
        [cond_missing, cond_amount, cond_neg_bal, cond_same_party, cond_type],
        ["MISSING_CRITICAL", "NON_POSITIVE_AMOUNT", "NEGATIVE_BALANCE", "SAME_SENDER_RECEIVER", "INVALID_TYPE"],
        default=None,
    )

    df["source_hash"] = df.apply(source_hash_row, axis=1)

    staging_transactions = df[df["reject_reason"].isna()].drop(columns=["reject_reason"]).copy()
    staging_transactions_rejects = df[df["reject_reason"].notna()].copy()

    # Stage 2: modeling
    parties = pd.Series(pd.concat([staging_transactions["nameOrig"], staging_transactions["nameDest"]], ignore_index=True).unique())
    parties = parties.dropna().astype(str).str.strip()
    parties = parties[parties.ne("")].drop_duplicates().to_frame(name="party_ref")

    dim_customer = parties.copy()
    dim_customer["customer_id"] = dim_customer["party_ref"].map(lambda x: det_id("CU", x))
    dim_customer["external_party_ref"] = dim_customer["party_ref"]
    dim_customer["customer_type"] = np.select(
        [dim_customer["party_ref"].str.startswith("C"), dim_customer["party_ref"].str.startswith("M")],
        ["RETAIL", "MERCHANT"],
        default="UNKNOWN",
    )
    dim_customer = dim_customer[["customer_id", "external_party_ref", "customer_type"]]

    open_step = pd.concat([
        staging_transactions[["step", "nameOrig"]].rename(columns={"nameOrig": "party_ref"}),
        staging_transactions[["step", "nameDest"]].rename(columns={"nameDest": "party_ref"}),
    ], ignore_index=True).groupby("party_ref", as_index=False)["step"].min().rename(columns={"step": "open_step"})

    dim_account = parties.merge(open_step, on="party_ref", how="left")
    dim_account["account_id"] = dim_account["party_ref"].map(lambda x: det_id("AC", x))
    dim_account["account_number"] = dim_account["party_ref"]
    dim_account["customer_id"] = dim_account["party_ref"].map(lambda x: det_id("CU", x))
    dim_account["account_type"] = np.select(
        [dim_account["party_ref"].str.startswith("C"), dim_account["party_ref"].str.startswith("M")],
        ["CUSTOMER", "MERCHANT"],
        default="UNKNOWN",
    )
    dim_account["account_status"] = "ACTIVE"
    dim_account = dim_account[["account_id", "account_number", "customer_id", "account_type", "account_status", "open_step"]]

    fact_transaction = staging_transactions.copy()
    fact_transaction = fact_transaction.rename(columns={"type": "transaction_type"})
    fact_transaction["origin_account_id"] = fact_transaction["nameOrig"].map(lambda x: det_id("AC", str(x)))
    fact_transaction["destination_account_id"] = fact_transaction["nameDest"].map(lambda x: det_id("AC", str(x)))
    fact_transaction = fact_transaction[[
        "source_hash", "step", "transaction_type", "amount", "origin_account_id", "destination_account_id",
        "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
    ]]

    # Stage 3: features + segments
    day0 = pd.Timestamp("2020-01-01")
    fact_transaction["txn_date"] = day0 + pd.to_timedelta(fact_transaction["step"], unit="D")
    hv_amount = fact_transaction["amount"].quantile(0.95)

    acct_map = dim_account[["account_id", "customer_id", "account_type"]].copy()

    outgoing = (
        fact_transaction.merge(
            acct_map.rename(columns={"account_id": "origin_account_id", "customer_id": "customer_id"}),
            on="origin_account_id", how="left"
        )
        .merge(
            acct_map.rename(columns={"account_id": "destination_account_id", "account_type": "counterparty_type"}),
            on="destination_account_id", how="left"
        )
    )
    outgoing["direction"] = "OUT"

    incoming = (
        fact_transaction.merge(
            acct_map.rename(columns={"account_id": "destination_account_id", "customer_id": "customer_id"}),
            on="destination_account_id", how="left"
        )
        .merge(
            acct_map.rename(columns={"account_id": "origin_account_id", "account_type": "counterparty_type"}),
            on="origin_account_id", how="left"
        )
    )
    incoming["direction"] = "IN"

    activity = pd.concat([
        outgoing[["customer_id", "source_hash", "txn_date", "amount", "isFraud", "direction", "counterparty_type"]],
        incoming[["customer_id", "source_hash", "txn_date", "amount", "isFraud", "direction", "counterparty_type"]],
    ], ignore_index=True)

    analysis_date = activity["txn_date"].max()
    g = activity.groupby("customer_id", dropna=False)

    mart_customer_features = g.agg(
        last_txn_date=("txn_date", "max"),
        frequency=("source_hash", "count"),
        monetary=("amount", "sum"),
        avg_txn_amount=("amount", "mean"),
        fraud_rate=("isFraud", "mean"),
    ).reset_index()

    mart_customer_features["high_value_ratio"] = g.apply(lambda x: (x["amount"] >= hv_amount).mean()).values
    mart_customer_features["merchant_exposure_ratio"] = g.apply(lambda x: (x["counterparty_type"] == "MERCHANT").mean()).values
    mart_customer_features["txns_7d"] = g.apply(lambda x: (x["txn_date"] >= (analysis_date - pd.Timedelta(days=7))).sum()).values
    mart_customer_features["txns_30d"] = g.apply(lambda x: (x["txn_date"] >= (analysis_date - pd.Timedelta(days=30))).sum()).values
    mart_customer_features["incoming_amount"] = g.apply(lambda x: x.loc[x["direction"] == "IN", "amount"].sum()).values
    mart_customer_features["outgoing_amount"] = g.apply(lambda x: x.loc[x["direction"] == "OUT", "amount"].sum()).values

    mart_customer_features["recency_days"] = (analysis_date - mart_customer_features["last_txn_date"]).dt.days
    mart_customer_features["velocity_7d_30d_ratio"] = mart_customer_features["txns_7d"] / mart_customer_features["txns_30d"].replace(0, np.nan)
    mart_customer_features["net_flow"] = mart_customer_features["incoming_amount"] - mart_customer_features["outgoing_amount"]
    mart_customer_features["as_of_date"] = analysis_date.normalize()

    mart_customer_features = mart_customer_features[[
        "customer_id", "recency_days", "frequency", "monetary", "avg_txn_amount", "fraud_rate", "high_value_ratio",
        "merchant_exposure_ratio", "txns_7d", "txns_30d", "velocity_7d_30d_ratio", "net_flow", "as_of_date",
    ]]

    seg = mart_customer_features.copy()
    seg["r_score"] = qscore(seg["recency_days"], reverse=True)
    seg["f_score"] = qscore(seg["frequency"], reverse=False)
    seg["m_score"] = qscore(seg["monetary"], reverse=False)
    seg["rfm_score_total"] = seg["r_score"] + seg["f_score"] + seg["m_score"]

    def segment_label(x):
        if x["fraud_rate"] >= 0.20:
            return "HIGH_RISK"
        if x["recency_days"] > 90 and x["txns_30d"] == 0:
            return "DORMANT"
        if x["r_score"] >= 4 and x["f_score"] >= 4 and x["m_score"] >= 4 and x["fraud_rate"] < 0.02:
            return "PREMIER"
        if x["merchant_exposure_ratio"] >= 0.70 and x["frequency"] >= 10:
            return "MERCHANT_HEAVY"
        vel = x["velocity_7d_30d_ratio"] if pd.notna(x["velocity_7d_30d_ratio"]) else 0
        if vel >= 0.60 and x["high_value_ratio"] >= 0.20:
            return "SURGING_HIGH_VALUE"
        if x["rfm_score_total"] >= 12:
            return "LOYAL_ACTIVE"
        if 9 <= x["rfm_score_total"] <= 11:
            return "GROWTH_POTENTIAL"
        if 6 <= x["rfm_score_total"] <= 8:
            return "MASS_MARKET"
        return "LOW_ENGAGEMENT"

    def risk_band(x):
        if x["fraud_rate"] >= 0.20 or x["high_value_ratio"] >= 0.50:
            return "HIGH"
        if x["fraud_rate"] >= 0.05 or x["high_value_ratio"] >= 0.20:
            return "MEDIUM"
        return "LOW"

    seg["segment_label"] = seg.apply(segment_label, axis=1)
    seg["risk_band"] = seg.apply(risk_band, axis=1)

    mart_customer_segments = seg[[
        "customer_id", "recency_days", "frequency", "monetary", "r_score", "f_score", "m_score", "rfm_score_total",
        "fraud_rate", "avg_txn_amount", "high_value_ratio", "merchant_exposure_ratio", "txns_7d", "txns_30d",
        "velocity_7d_30d_ratio", "net_flow", "segment_label", "risk_band", "as_of_date",
    ]]

    # Outputs
    staging_transactions.to_csv("staging_transactions.csv", index=False)
    staging_transactions_rejects.to_csv("staging_transactions_rejects.csv", index=False)
    dim_customer.to_csv("dim_customer.csv", index=False)
    dim_account.to_csv("dim_account.csv", index=False)
    fact_transaction.to_csv("fact_transaction.csv", index=False)
    mart_customer_features.to_csv("mart_customer_features.csv", index=False)
    mart_customer_segments.to_csv("mart_customer_segments.csv", index=False)


if __name__ == "__main__":
    main()
