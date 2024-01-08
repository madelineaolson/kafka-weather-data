import pandas as pd
import os
import json
import matplotlib.pyplot as plt


months = ["January", "February", "March"]
avg_temps = {month: None for month in months}
max_year = {"January":0, "February":0, "March":0}
for partition in range(4):
    file_path = f"/files/partition-{partition}.json"
    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            data = json.load(file)
            for key in data.keys():
                if key in months:
                    
                    if data[key]:
                        year = max(data[key].keys(), key=lambda y: int(y))
                        if int(year) > int(max_year[key]):
                            max_year[key] = year
                            avg_max_temp = data[key][year]["avg"]
                            dict_key = f"{key}-{year}"
                            avg_temps[key] = avg_max_temp
                        else:
                            pass


avg_temps = {k: v for k, v in avg_temps.items() if v is not None}
final_temps = {}
for i in avg_temps.keys():
    final_temps[i + '-' + max_year[i]] = avg_temps[i]

month_series = pd.Series(final_temps)


fig, ax = plt.subplots()
month_series.plot.bar(ax=ax)
ax.set_ylabel('Avg. Max Temperature')
plt.tight_layout()
plt.savefig("/files/month.svg")
