from scipy import stats

# Example dummy values for illustration
accuracy_v1 = [0.78, 0.80, 0.81, ...]  # 50 values
accuracy_v2 = [0.80, 0.82, 0.85, ...]

# Paired t-test
t_stat, p_value = stats.ttest_rel(accuracy_v2, accuracy_v1)

print(f"T-statistic: {t_stat}")
print(f"P-value: {p_value}")

