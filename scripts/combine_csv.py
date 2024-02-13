# importing pandas
import pandas as pd
from os import listdir
from os.path import isfile, join


# if input("are you sure? (y/n)") != "y":
#     exit()

# PUSH SCAN FILES ...
path = "files/Para20/2020/"
onlyfilesah = [path+f for f in listdir(path) if isfile(join(path, f))]

df1 = pd.concat(
    map(pd.read_csv, onlyfilesah), ignore_index=True)

df1 = df1.sort_values(by=["Date", "Ticker", "Scan Time"])
df1 = df1.drop_duplicates(
    subset=['Date', 'Ticker'],
    keep='first').reset_index(drop=True)
# df1["Scan Type"] = "38% Push"

df1.to_csv(f"{path}/Para20_2020.csv", index=False)

# AH SCAN FILES ...
# ahpath = "files/ah_scans_filtered/"
# onlyfilesah = [ahpath+f for f in listdir(ahpath) if isfile(join(ahpath, f))]
#
# ah_df = pd.concat(
#     map(pd.read_csv, onlyfilesah), ignore_index=True)
#
# ah_df = ah_df.sort_values(by=["Date", "Ticker"])
# ah_df = ah_df.drop_duplicates()
# ah_df["Scan Type"] = "Aftn % Push"
# ah_df.to_csv(f"{ahpath}/ah_combined_38_2.csv", index=False)


# df3 = pd.concat([df, df2], ignore_index=True)
# df3 = df3.drop_duplicates(subset=["Date", "Ticker"], keep='last')
# df3.to_csv(f"{mypath}/push and ah_combined.csv", index=False)


# COMBINING FILES PUSH & AFTN PUSH...
# master_df = pd.read_csv("files/2022 - 2022-day2-strat.csv")
# master_df = master_df.sort_values(by=["date", "ticker"])
# master_df["date"] = pd.to_datetime(master_df["date"]).dt.strftime("%Y-%m-%d")
# master_df["key"] = master_df["date"]+master_df["ticker"]

#
# push_df = pd.read_csv("files/perc_push_all_2/pushperc38filtered.csv")
# push_df["key"] = push_df["Date"]+push_df["Ticker"]
#
# aftn_df = pd.read_csv("files/ah_scans_filtered/ah_combined_38_2.csv")
# aftn_df["key"] = aftn_df["Date"]+aftn_df["Ticker"]
#
# df = pd.concat([push_df, aftn_df], ignore_index=True)
# df = df.drop_duplicates(subset=["Date", "Ticker"])
# #
# #
# comb_df = df.merge(master_df, how="left", left_on="key", right_on="key")



# print()
#
# comb_df.to_csv(f"files/push data.csv", index=False)

print("done.")
