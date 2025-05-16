def t_confidence_single_group(data, alpha=0.05):
    
    data = np.array(data)
    n = len(data)
    mean = np.mean(data)
    sem = np.std(data, ddof=1) / np.sqrt(n)
    t_crit = t.ppf(1 - alpha / 2, df=n - 1)

    ci_lower = mean - t_crit * sem
    ci_upper = mean + t_crit * sem

    return {
        "mean": round(mean,3),
        "ci": (round(ci_lower,3), round(ci_upper,3))
    }


def welchs_ttest_with_ci(df: pd.DataFrame, adjusted_metric: str, alpha: float = 0.05):
    
    group_a = df[df["is_customer_holdout"] == False][adjusted_metric].values
    group_b = df[df["is_customer_holdout"] == True][adjusted_metric].values

    # Means and sizes
    mean_a, mean_b = np.mean(group_a), np.mean(group_b)
    n_a, n_b = len(group_a), len(group_b)
    var_a, var_b = np.var(group_a, ddof=1), np.var(group_b, ddof=1)

    # Welch's t-test
    t_stat, p_value = ttest_ind(group_a, group_b, equal_var=False)

    # Observed difference
    observed_diff = mean_a - mean_b

    # Standard error of the difference
    se_diff = np.sqrt(var_a / n_a + var_b / n_b)

    # Degrees of freedom (Welch-Satterthwaite equation)
    df_denom = (var_a / n_a + var_b / n_b) ** 2
    df_numer = ((var_a / n_a) ** 2) / (n_a - 1) + ((var_b / n_b) ** 2) / (n_b - 1)
    df_eff = df_denom / df_numer

    # t critical value
    t_crit = t.ppf(1 - alpha / 2, df=df_eff)

    # Confidence interval
    ci_lower = observed_diff - t_crit * se_diff
    ci_upper = observed_diff + t_crit * se_diff

    mean_holdout = np.mean(group_b)

    if mean_holdout != 0:
        pct_diff = observed_diff / abs(mean_holdout)
        pct_ci_low = ci_lower / abs(mean_holdout)
        pct_ci_high = ci_upper / abs(mean_holdout)
    else:
        pct_diff = pct_ci_low = pct_ci_high = np.nan

    return {
        "observed_diff": observed_diff,
        "t_statistic": t_stat,
        "p_value": p_value,
        "ci": (ci_lower, ci_upper),
        "se": se_diff,
        "pct_diff": pct_diff,
        "pct_ci": (pct_ci_low, pct_ci_high)
    }