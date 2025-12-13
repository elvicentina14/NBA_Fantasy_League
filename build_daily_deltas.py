import pandas as pd

df = pd.read_csv("player_season_totals.csv", dtype=str)

df["stat_value"] = pd.to_numeric(df["stat_value"], errors="coerce")
df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])

df = df.sort_values(["player_key", "stat_id", "snapshot_date"])

df["daily_value"] = (
    df.groupby(["player_key", "stat_id"])["stat_value"]
      .diff()
      .fillna(df["stat_value"])
)

out = df[[
    "player_key",
    "stat_id",
    "snapshot_date",
    "daily_value"
]]

out.to_csv("player_daily_deltas.csv", index=False)
print("✅ player_daily_deltas.csv written")
