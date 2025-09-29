import pandas as pd
import matplotlib.pyplot as plt

global_data = pd.read_csv("global_disaster_aid_gaps/global_disaster_aid_gaps_2014_2024.csv")
largest_abs = pd.read_csv("global_disaster_aid_gaps/top_countries_largest_absolute_gaps.csv")
smallest_abs = pd.read_csv("global_disaster_aid_gaps/top_countries_smallest_absolute_gaps.csv")

global_trend = global_data.groupby("year")["gap"].sum().reset_index()

# Global Disaster Aid Gap Trend Line Graph
plt.figure(figsize=(9, 5))
plt.plot(global_trend["year"], global_trend["gap"]/1e9, marker="o", color="steelblue", linewidth=2)
plt.fill_between(global_trend["year"], global_trend["gap"]/1e9, alpha=0.2, color="steelblue")
plt.title("Global Disaster Aid Gap Trend (2014–2024)", fontsize=14, weight="bold")
plt.xlabel("Year")
plt.ylabel("Aid Gap (Billion USD)")
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig("fig_global_trend.png", dpi=300)
plt.close()

# Top 3 Largest Disaster Aid Gaps Bar Chart
plt.figure(figsize=(7, 4))
plt.barh(largest_abs["country"], largest_abs["total_gap"]/1e9, color="indianred")
plt.title("Top 3 Largest Disaster Aid Gaps (Absolute, 2014–2024)", fontsize=13, weight="bold")
plt.xlabel("Aid Gap (Billion USD)")
plt.gca().invert_yaxis()  # biggest on top
for i, v in enumerate(largest_abs["total_gap"]/1e9):
    plt.text(v + 0.2, i, f"{v:.1f}", va="center")
plt.tight_layout()
plt.savefig("fig_top3_largest.png", dpi=300)
plt.close()

# Top 3 Smallest Disaster Aid Gaps Bubble Chart
# The x-axis represents the funding_percentage
# the y-axis represents the total_gap
# the size of the bubbles represents the total_people_affected.
plt.figure(figsize=(7, 5))
sizes = smallest_abs["total_people_affected"] / 1000  # zoom the bubble size
scatter = plt.scatter(
    smallest_abs["funding_percentage"], 
    smallest_abs["total_gap"]/1e6, 
    s=sizes, 
    c=smallest_abs["gap_percentage"], 
    cmap="viridis", 
    alpha=0.7, 
    edgecolors="black"
)

for i, row in smallest_abs.iterrows():
    plt.text(row["funding_percentage"]+1, row["total_gap"]/1e6, row["country"], fontsize=9)

plt.title("Top 3 Smallest Disaster Aid Gaps (Absolute, 2014–2024)", fontsize=13, weight="bold")
plt.xlabel("Funding Percentage (%)")
plt.ylabel("Aid Gap (Million USD)")
cbar = plt.colorbar(scatter)
cbar.set_label("Gap Percentage (%)")
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig("fig_top3_smallest.png", dpi=300)
plt.close()

print("✅fig generated: fig_global_trend.png, fig_top3_largest.png, fig_top3_smallest.png")
