import numpy as np
import scipy.stats as st
import matplotlib.pyplot as plt

# Data provided by the user (re-using from previous context)
all_data = open("/tmp/sample_data.txt").readlines()
labels = all_data[0].split("|")
print(f"labels are {labels}")

datasets = []
for i in range(1, len(all_data)):
    datasets.append([float(x.rstrip()) for x in all_data[i].split(",")])

# --- Calculate Statistics (same as before) ---
means = []
ci_errors = []

for data in datasets:
    n = len(data)
    mean = np.mean(data)
    std_dev = np.std(data, ddof=1)
    sem = std_dev / np.sqrt(n)
    df = n - 1
    t_score = st.t.ppf(0.975, df)
    margin_of_error = t_score * sem

    means.append(mean)
    ci_errors.append(margin_of_error)

# --- Plotting with requested changes ---

plt.figure(figsize=(10, 6))

bars = plt.bar(
    labels,
    means,
    yerr=ci_errors,
    capsize=5,
    # color=["blue", "green", "red"],
    alpha=0.7,
    width=0.2,
)

# Add titles and labels
plt.title("Mean of Point Reads with 95% Confidence Intervals Neon", fontsize=16)

# Change 1: Update Y-axis label
plt.ylabel("Mean Read Time in ms", fontsize=12)
plt.xlabel("", fontsize=12)

# Add text labels for means on top of bars
for bar, mean_val in zip(bars, means):
    yval = bar.get_height()
    plt.text(
        bar.get_x() + bar.get_width() / 2.0,
        yval + 0.5,
        f"{mean_val:.2f}",
        ha="center",
        va="bottom",
    )

# Adjust layout
plt.tight_layout()

# Save the plot
plt.savefig("mean_reads_confidence_interval_plot_updated.png")

print("Plot saved as 'mean_reads_confidence_interval_plot_updated.png'")
