!pip install google-cloud-bigquery pandas scipy statsmodels --quiet

from google.colab import auth
from google.cloud import bigquery
import pandas as pd
from statsmodels.stats.proportion import proportions_ztest
from scipy.stats import ttest_ind
from datetime import date


#connection to my Bigquery repo to pull the data
auth.authenticate_user()

project_id = "elegant-shelter-407900"
client = bigquery.Client(project=project_id)


# I'll qury the pre-aggregated table I'm connecting already to Looker Studio to show the same data there
query = """
SELECT
  cohort AS variant
  , user_id
  , session_id
  , step_3_flg
  , event_count_Search_Sub
  , case when step_4_flg > 0 then 1 else 0 end as step_4_flg
  , case when step_5_flg > 0 then 1 else 0 end as step_5_flg
  , step_6_flg as step_6_flg

FROM `elegant-shelter-407900.hostelworld.ab_test_base_sessions_pre_agregg`
WHERE step_2_flg > 0
"""

df = client.query(query).to_dataframe()


# I'll check the three main CR asked in the take home task
# Conversion metrics
conversion_metrics = [
    "step_4_flg",
    "step_5_flg",
    "step_6_flg"
]

# Average-per-session metrics (numeric)
avg_metrics = [
    "event_count_Search_Sub"  
]

test_name = "Hostelworld - Enhanced Autocomplete Search with Personalized Recommendations"
alpha = 0.05



results = []

# running the test and storing the results in a table so I can send it back to BQ and use the statistical significance data in Looker Studio directly
for metric in conversion_metrics:
    summary = (
        df
        .groupby("variant")
        .agg(
            conversions=(metric, "sum"),
            total_sessions=(metric, "count")
        )
    )

    summary["value"] = summary["conversions"] / summary["total_sessions"]

    z_stat, p_value = proportions_ztest(
        count=summary["conversions"].values,
        nobs=summary["total_sessions"].values
    )

    for variant, row in summary.iterrows():
        results.append({
            "test_name": test_name,
            "metric": metric,
            "metric_type": "conversion_rate",
            "variant": variant,
            "value": float(row["value"]),
            "sample_size": int(row["total_sessions"]),
            "statistic": float(z_stat),
            "p_value": float(p_value),
            "is_significant": p_value < alpha,
            "run_date": date.today()
        })

        
for metric in avg_metrics:
    filtered = df[df["step_3_flg"] == 1]

    per_user = (
        filtered
        .groupby(["variant", "user_id"])[metric]
        .sum()
        .reset_index()
    )

    group_a = per_user[per_user["variant"] == "Variation"][metric]
    group_b = per_user[per_user["variant"] == "Control"][metric]

    mean_a = group_a.mean()
    mean_b = group_b.mean()

    t_stat, p_value = ttest_ind(
        group_a,
        group_b,
        equal_var=False
    )

    results.extend([
        {
            "test_name": test_name,
            "metric": metric,
            "metric_type": "avg_events_per_user",
            "variant": "Variation",
            "value": float(mean_a),
            "sample_size": int(group_a.nunique()),
            "statistic": float(t_stat),
            "p_value": float(p_value),
            "is_significant": p_value < alpha,
            "run_date": date.today()
        },
        {
            "test_name": test_name,
            "metric": metric,
            "metric_type": "avg_events_per_user",
            "variant": "Control",
            "value": float(mean_b),
            "sample_size": int(group_b.nunique()),
            "statistic": float(t_stat),
            "p_value": float(p_value),
            "is_significant": p_value < alpha,
            "run_date": date.today()
        }
    ])


# results in table format:
results_df = pd.DataFrame(results)

print("Final results preview:")
print(results_df)


# exporting back to BQ:
table_id = "elegant-shelter-407900.hostelworld.ab_test_stat_sig_results_CR_2"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND"
)

job = client.load_table_from_dataframe(
    results_df,
    table_id,
    job_config=job_config
)

job.result()

print(f"✅ Results successfully written to {table_id}")
