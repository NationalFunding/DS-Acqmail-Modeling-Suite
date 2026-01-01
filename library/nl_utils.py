# ============================================================
# Portable Utilities (Non-Databricks Version)
# ============================================================

import base64
import io
import math
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from scipy.stats import chi2_contingency

# ------------------------------------------------------------
# Portable HTML display helpers (Databricks replacement)
# ------------------------------------------------------------
try:
    from IPython.display import display, HTML

    def displayHTML(html):
        display(HTML(html))

except ImportError:
    def displayHTML(html):
        print(html)


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


# ============================================================
# FILE READERS (Spark → Pandas)
# ============================================================

def readin_csv(file_location):
    """
    Replacement for spark.read.csv
    Returns pandas DataFrame
    """
    return pd.read_csv(file_location)


def readin_txt(file_location):
    """
    Replacement for spark.read.csv (txt treated as csv)
    Returns pandas DataFrame
    """
    return pd.read_csv(file_location)


# ============================================================
# WRITE DATA (Spark Table → File)
# ============================================================

def write_pyspark2DB(df, permanent_table_name, partitionBy=True):
    """
    Databricks replacement:
    Writes dataframe to disk instead of Hive table.
    """

    filename = permanent_table_name.replace(".", "_")

    if partitionBy and {"load_year", "load_month"}.issubset(df.columns):
        out_path = f"{filename}_partitioned.csv"
    else:
        out_path = f"{filename}.csv"

    df.to_csv(out_path, index=False)
    print(f"[INFO] Data written to {out_path}")


# ============================================================
# BIVARIATE PLOT
# ============================================================

def bivariate_plot(
    df,
    label_col,
    by_col,
    investigate_cols,
    cat_cols,
    num_cols,
    figsize
):
    ncols = 2
    fig, axes = plt.subplots(
        nrows=math.ceil(len(investigate_cols) / ncols) * 2,
        ncols=ncols,
        figsize=figsize
    )
    ax = axes.flatten()

    for i, col in enumerate(investigate_cols):

        if df[col].notna().sum() == 0:
            continue

        if col in cat_cols:
            total = df.groupby([col, by_col])['duns'].count().reset_index()
            summary = df[df[label_col] == 1].groupby([col, by_col])['duns'].count().reset_index()

        if col in num_cols:
            df['binned'], bins = pd.qcut(df[col], 20, retbins=True, duplicates='drop')
            if len(bins) == 1 or any(x in col for x in ['ind', 'chg_', 'cadence_', 'mailed_over_12mo', 'flg_']):
                df['binned'] = df[col]

            dataset = df[[label_col, 'duns', by_col, 'binned']].copy()
            dataset.rename(columns={'binned': col}, inplace=True)

            total = dataset.groupby([col, by_col])['duns'].count().reset_index()
            summary = dataset[dataset[label_col] == 1].groupby([col, by_col])['duns'].count().reset_index()

        summary = total.merge(summary, how='left', on=[col, by_col])
        summary['rate_resp'] = (summary['duns_y'] / summary['duns_x']) * 100
        summary.columns = [col, by_col, 'count', label_col, 'rate_resp']
        summary['flg_noresp'] = summary['count'] - summary[label_col]

        summary_T = summary[summary[by_col] == 1]
        summary_notT = summary[summary[by_col] == 0]

        try:
            _, p_T, _, _ = chi2_contingency(summary_T[['flg_noresp', label_col]])
            _, p_notT, _, _ = chi2_contingency(summary_notT[['flg_noresp', label_col]])
        except Exception:
            p_T = p_notT = 1

        summary_T = summary_T[summary_T['count'] >= 300]
        summary_notT = summary_notT[summary_notT['count'] >= 300]

        ylim = max(summary_T['rate_resp'].max(), summary_notT['rate_resp'].max())

        sns.barplot(x=col, y="rate_resp", data=summary_notT, ax=ax[i * 2])
        ax[i * 2].set_ylim(0, ylim + 1)
        ax[i * 2].set_xlabel(col + ('*' if p_notT < 0.01 else ''))

        sns.barplot(x=col, y="rate_resp", data=summary_T, ax=ax[i * 2 + 1])
        ax[i * 2 + 1].set_ylim(0, ylim + 1)
        ax[i * 2 + 1].set_xlabel(col + ('*' if p_T < 0.01 else ''))

    plt.tight_layout()
    plt.show()


# ============================================================
# HEATMAP
# ============================================================

def heatmap(df, vars, size_r=5, size_c=5):
    fig, ax = plt.subplots(figsize=(size_r, size_c))
    sns.heatmap(df[vars].corr(), annot=True, ax=ax)
    plt.show()


# ============================================================
# HTML DISPLAY HELPERS
# ============================================================

def displayHTML_formated_background_color(input_df, inFormat, inAlign='right'):
    html = (
        input_df.style
        .format(inFormat)
        .set_properties(**{'text-align': inAlign})
        .background_gradient(cmap='Blues', axis=None)
        .to_html()
    )
    displayHTML(html)


def displayHTML_formated(input_df, inFormat):

    def formatters(df, fmt):
        return {
            c: fmt.format
            for c, t in df.dtypes.items()
            if t in [np.dtype('int64'), np.dtype('float64')]
        }

    displayHTML(input_df.to_html(formatters=formatters(input_df, inFormat)))
