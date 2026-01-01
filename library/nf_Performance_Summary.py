# ============================================================

import base64
import io
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn import metrics

# ------------------------------------------------------------
try:
    from IPython.display import display, HTML

    def displayHTML(html):
        display(HTML(html))

    def displayHTML_formated(df, fmt):
        display(df.style.format(fmt))

    def displayHTML_formated_background_color(df, fmt):
        display(df.style.format(fmt).background_gradient(axis=None))

except ImportError:
    # Script / non-notebook fallback
    def displayHTML(html):
        print(html)

    def displayHTML_formated(df, fmt):
        print(df)

    def displayHTML_formated_background_color(df, fmt):
        print(df)


# ------------------------------------------------------------
# Display matplotlib figure as embedded HTML image
# ------------------------------------------------------------
def display2(fig):
    plt.show()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    image = base64.b64encode(buf.read()).decode("utf-8")
    buf.close()
    displayHTML(f"""<img src="data:image/png;base64,{image}"></img>""")


# ------------------------------------------------------------
# Summary table generator
# ------------------------------------------------------------
def gen_summary_tab(df, var, target_label, sort_ascending=True, full=True):

    df = df.copy()

    df['large_fund'] = (df['fund_amt'] >= 25000).astype(int)
    df['large_ags'] = (df['lead_ags'] >= 250000).astype(int)

    measure_dict = {
        'flg_resp': ['count', 'sum'],
        'flg_qual': ['sum'],
        'flg_sub': ['sum'],
        'flg_appr': ['sum'],
        'flg_NFappr': ['sum'],
        'flg_fund': ['sum'],
        'flg_NFfund': ['sum'],
        'fund_amt': ['sum'],
        'NFfund_amt': ['sum'],
        'large_fund': ['sum'],
        'large_ags': ['sum'],
        'fund_margin': ['sum'],
        'mail_cnt': ['sum'],
        'ref_score': ['min', 'max']
    }

    score_col_nm = f"score_{target_label.replace('flg_', '').upper()}"
    measure_dict[score_col_nm] = ['min', 'max']

    summary = df.groupby(var, dropna=False).agg(measure_dict)
    summary.sort_index(inplace=True, ascending=sort_ascending)

    summary['cnt'] = summary['flg_resp']['count']
    summary['cumu_cnt'] = summary['cnt'].cumsum()

    summary['cumu_resp'] = summary['flg_resp']['sum'].cumsum()
    summary['cumu_resp_pcnt'] = summary['cumu_resp'] / summary['flg_resp']['sum'].sum()

    summary['resp_rate'] = (100 * summary['flg_resp']['sum'] / summary['cnt']).round(2)
    summary['qual_rate'] = (100 * summary['flg_qual']['sum'] / summary['cnt']).round(2)
    summary['sub_rate'] = (100 * summary['flg_sub']['sum'] / summary['cnt']).round(2)
    summary['appr_rate'] = (100 * summary['flg_appr']['sum'] / summary['cnt']).round(2)
    summary['fund_rate'] = (100 * summary['flg_fund']['sum'] / summary['cnt']).round(2)

    summary['avg_funded'] = (summary['fund_amt']['sum'] / summary['flg_fund']['sum']).round(0)
    summary['cumu_fund_amt'] = summary['fund_amt']['sum'].cumsum()
    summary['cumu_fund_amt_pcnt'] = summary['cumu_fund_amt'] / summary['fund_amt']['sum'].sum()

    if full:
        return summary
    else:
        target_short = target_label.replace('flg_', '')
        return summary[['cnt', 'resp_rate', 'sub_rate', 'appr_rate', 'fund_rate',
                        'cumu_cnt', 'cumu_fund_amt_pcnt']]


# ------------------------------------------------------------
# ROC curve helper
# ------------------------------------------------------------
def create_roc(y_true, y_score):
    fpr, tpr, thresholds = metrics.roc_curve(y_true, y_score, pos_label=1)
    gmeans = np.sqrt(tpr * (1 - fpr))
    ix = np.argmax(gmeans)

    print(f"Best Threshold={thresholds[ix]:.4f}, G-Mean={gmeans[ix]:.3f}")

    roc_auc = metrics.auc(fpr, tpr)

    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(fpr, tpr, label=f"AUC = {roc_auc:.3f}")
    ax.scatter(fpr[ix], tpr[ix])
    ax.plot([0, 1], [0, 1], 'r--')
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_xlabel("False Positive Rate")
    ax.set_ylabel("True Positive Rate")
    ax.legend(loc="lower right")
    plt.show()


# ------------------------------------------------------------
# Full performance report
# ------------------------------------------------------------
def report_performance(df, col_proba, target_label, report_full_metrics=True):

    dff = df.copy()
    dff['bestmodel_score'] = dff[col_proba]
    dff['model_pred'] = (dff['bestmodel_score'] >= 0.5).astype(int)

    score_bins = np.arange(0, 1.1, 0.1)
    dff['bestmodel_scorebin'] = pd.cut(dff['bestmodel_score'], bins=score_bins, right=False)
    dff['bestmodel_decile'] = (
        pd.qcut(dff['bestmodel_score'].rank(method='first'), q=10)
        .rank(method='dense', ascending=False)
    )

    print("BEST MODEL METRICS")
    print("=" * 40)
    print("ROC AUC:", metrics.roc_auc_score(dff[target_label], dff['bestmodel_score']).round(3))
    print("Accuracy:", metrics.accuracy_score(dff[target_label], dff['model_pred']).round(3))
    print("Precision:", metrics.precision_score(dff[target_label], dff['model_pred']).round(3))
    print("Recall:", metrics.recall_score(dff[target_label], dff['model_pred']).round(3))
    print("F1:", metrics.f1_score(dff[target_label], dff['model_pred']).round(3))

    g = sns.FacetGrid(dff, hue=target_label, palette=['red', 'green'])
    g.map(sns.kdeplot, "bestmodel_score")
    plt.show()

    create_roc(dff[target_label], dff['bestmodel_score'])

    summary = gen_summary_tab(dff, 'bestmodel_scorebin', target_label, False, report_full_metrics)
    displayHTML(summary.to_html())

    summary = gen_summary_tab(dff, 'bestmodel_decile', target_label, True, report_full_metrics)
    displayHTML(summary.to_html())

    return summary


# ------------------------------------------------------------
# Crosstab utilities
# ------------------------------------------------------------
def performance_crosstab(dff, col1, col2, ascending=[True, True]):

    print("=" * 50 + " COUNT")
    tab = pd.crosstab(dff[col1], dff[col2])
    tab = tab.sort_index(ascending=ascending[0])
    displayHTML_formated(tab, "{:,}")

    print("=" * 50 + " ROW %")
    tab = pd.crosstab(dff[col1], dff[col2], normalize='index') * 100
    displayHTML_formated_background_color(tab, "{:.1f}%")

    print("=" * 50 + " COLUMN %")
    tab = pd.crosstab(dff[col1], dff[col2], normalize='columns') * 100
    displayHTML_formated_background_color(tab, "{:.1f}%")


def performance_crosstab_without_performance(dff, col1, col2, ascending=[True, True]):

    print("=" * 50 + " COUNT")
    tab = pd.crosstab(dff[col1], dff[col2])
    displayHTML_formated(tab, "{:,}")

    print("=" * 50 + " COLUMN %")
    tab = pd.crosstab(dff[col1], dff[col2], normalize='columns') * 100
    displayHTML_formated_background_color(tab, "{:.1f}%")

    print("=" * 50 + " ROW %")
    tab = pd.crosstab(dff[col1], dff[col2], normalize='index') * 100
    displayHTML_formated_background_color(tab, "{:.1f}%")
