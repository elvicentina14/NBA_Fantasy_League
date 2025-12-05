# combine_outputs.py
import pandas as pd
import os
# load files
if not os.path.exists("team_rosters.csv"):
    print("team_rosters.csv not found")
if not os.path.exists("player_stats.csv"):
    print("player_stats.csv not found")
if not os.path.exists("league_players.csv"):
    print("league_players.csv not found (optional)")

df_rosters = pd.read_csv("team_rosters.csv", dtype=str)
df_stats = pd.read_csv("player_stats.csv", dtype=str)

# normalize column names
df_rosters = df_rosters.rename(columns=lambda c: c.strip())
df_stats = df_stats.rename(columns=lambda c: c.strip())

# merge on player_key; fallback to player_name if player_key missing
merged = pd.merge(df_stats, df_rosters[['player_key','team_key','team_name','position']], how='left', on='player_key')

# fallback join on player_name where team is missing
missing_team = merged[merged['team_name'].isna()]
if not missing_team.empty:
    fallback = pd.merge(missing_team.drop(columns=['team_key','team_name','position']), df_rosters[['player_name','team_key','team_name','position']],
                        how='left', left_on='player_name', right_on='player_name', suffixes=('','_r'))
    # update rows in merged where team_name is null
    for idx, row in fallback.iterrows():
        orig_idx = merged[merged['player_key'] == row['player_key']].index
        for i in orig_idx:
            if pd.isna(merged.at[i,'team_name']) and not pd.isna(row['team_name']):
                merged.at[i,'team_key'] = row['team_key']
                merged.at[i,'team_name'] = row['team_name']
                merged.at[i,'position'] = row.get('position')

# reorder columns
cols = ['player_key','player_name','team_key','team_name','position','stat_id','stat_value']
merged = merged[[c for c in cols if c in merged.columns]]

# write combined view
merged.to_csv("combined_player_view.csv", index=False)
print("Wrote combined_player_view.csv rows:", len(merged))
