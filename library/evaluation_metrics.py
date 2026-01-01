import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score
)

from IPython.display import display,HTML

from library.nf_Performance_Summary import gen_summary_tab  # adjust if needed


def gen_evaluation_metrics_new(
    dff,
    score_col_name,
    metric,
    optimize_threshold=True,
    min_precision=None
):
    dff = dff.copy()

    dff[metric] = pd.to_numeric(dff[metric], errors='coerce')
    dff['bestmodel_score'] = dff[score_col_name]

    dff['model_pred'] = (dff['bestmodel_score'] >= 0.5).astype(int)

    score_bins = np.arange(0, 1.1, 0.1)
    dff['bestmodel_scorebin'] = pd.cut(
        dff['bestmodel_score'],
        bins=score_bins,
        right=False
    )

    dff['bestmodel_decile'] = (
        pd.qcut(dff['bestmodel_score'], q=10, duplicates='drop')
          .rank(method='dense', ascending=False)
    )

    dff['large_fund'] = (dff['fund_amt'] >= 25000).astype(int)

    # KDE plot
    g = sns.FacetGrid(dff, hue=metric, palette=['red', 'green'])
    g.map(sns.kdeplot, "bestmodel_score")
    plt.title(f"Score distribution by {metric}")
    plt.show()

    # Metrics
    acc = accuracy_score(dff[metric], dff['model_pred'])
    prec_val = precision_score(dff[metric], dff['model_pred'])
    rec_val = recall_score(dff[metric], dff['model_pred'])
    f1_val = f1_score(dff[metric], dff['model_pred'])
    roc_auc_val = roc_auc_score(dff[metric], dff['bestmodel_score'])

    print(f"roc_auc: {roc_auc_val:.3f}")
    print(f"accuracy: {acc:.3f}")
    print(f"precision: {prec_val:.3f}")
    print(f"recall: {rec_val:.3f}")
    print(f"f1_score: {f1_val:.3f}")

    # Capture rates
    df_sorted = dff.sort_values('bestmodel_score', ascending=False)
    total_positives = df_sorted[metric].sum()

    for pct in [0.10, 0.20]:
        top = df_sorted.head(int(len(df_sorted) * pct))
        capture = top[metric].sum() / total_positives if total_positives > 0 else np.nan
        print(f"capture_rate_{int(pct*100)}%: {capture:.3f}")

    for col in ['flg_resp', 'flg_sub', 'flg_appr', 'flg_fund', 'fund_amt']:
        if col in dff.columns:
            dff[col] = dff[col].astype(int)

    scorebin_df = gen_summary_tab(
        df=dff,
        var='bestmodel_scorebin',
        target_label=metric,
        sort_ascending=False,
        full=True
    )

    decile_df = gen_summary_tab(
        df=dff,
        var='bestmodel_decile',
        target_label=metric,
        sort_ascending=True,
        full=True
    )

    # Convert Interval columns to strings for HTML display
    for df in (scorebin_df, decile_df):
        for col in df.select_dtypes(include=['interval']).columns:
            df[col] = df[col].astype(str)

    displayHTML(scorebin_df.to_html())
    displayHTML(decile_df.to_html())

    return {
        "roc_auc": roc_auc_val,
        "accuracy": acc,
        "precision": prec_val,
        "recall": rec_val,
        "f1": f1_val
    }
