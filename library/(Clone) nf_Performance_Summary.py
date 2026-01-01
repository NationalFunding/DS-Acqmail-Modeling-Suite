# Databricks notebook source
# MAGIC %run ./nl_utils

# COMMAND ----------

import base64
import io
import seaborn as sns
from sklearn import metrics
import matplotlib.pyplot as plt
import numpy as np

# COMMAND ----------

# fig is a matplotlib.figure.Figure object
def display2(fig):
  plt.show()
  buf = io.BytesIO()
  fig.savefig(buf, format="png")
  buf.seek(0)
  image = base64.b64encode(buf.read()).decode("utf-8")
  buf.close()
  displayHTML(f"""<img src="data:image/png;base64,{image}"></img>""")

# COMMAND ----------

def gen_summary_tab(df, var, target_label, sort_ascending = True, full = True):
  
    df = df.copy()
    df['large_fund'] = 0
    df.loc[df['fund_amt'] >= 25000, 'large_fund'] = 1

    df['large_ags'] = 0
    df.loc[df['lead_ags'] >= 250000, 'large_ags'] = 1
          
        
    #################################################
    # decide measure list
    #################################################
    measure_dict = {'flg_resp':['count', 'sum'], 'flg_qual':['sum'], 'flg_sub':['sum'], 'flg_appr':['sum'], 'flg_NFappr':['sum'], 'flg_fund':['sum'], 'flg_NFfund':['sum'], 
                    'fund_amt':['sum'],'NFfund_amt':['sum'],'large_fund':['sum'],'large_ags':['sum'], 'fund_margin':['sum'], 'mail_cnt':['sum']}
    
    # if any([c for c in df.columns if 'TIB' in c])==True:
    #     measure_dict['TIB'] = ['mean']
    # if any([c for c in df.columns if 'gc_employees' in c])==True:
    #     measure_dict['gc_employees'] = ['mean']
    # if any([c for c in df.columns if 'gc_sales' in c])==True:
    #     measure_dict['gc_sales'] = ['mean']
        
    if any([c for c in df.columns if 'score_RESP' in c])==True:
        measure_dict['score_RESP'] = ['min', 'max']
        
    
    
    #################################################
    # group by
    #################################################
    summary = df.groupby(var, dropna=False).agg(measure_dict)
    summary.sort_index(inplace=True, ascending=sort_ascending)

    summary['cnt'] = summary['flg_resp']['count']
    summary['cumu_cnt'] = summary['flg_resp']['count'].cumsum()
    summary['cumu_resp'] = summary['flg_resp']['sum'].cumsum()
    summary['cumu_resp_pcnt'] = summary['flg_resp']['sum'].cumsum()/summary['flg_resp']['sum'].sum()
    summary['cumu_qual'] = summary['flg_qual']['sum'].cumsum()
    summary['cumu_qual_pcnt'] = summary['flg_qual']['sum'].cumsum()/summary['flg_qual']['sum'].sum()
    summary['cumu_sub'] = summary['flg_sub']['sum'].cumsum()
    summary['cumu_sub_pcnt'] = summary['flg_sub']['sum'].cumsum()/summary['flg_sub']['sum'].sum()
    summary['cumu_appr'] = summary['flg_appr']['sum'].cumsum()
    summary['cumu_appr_pcnt'] = summary['flg_appr']['sum'].cumsum()/summary['flg_appr']['sum'].sum()
    summary['cumu_NFappr'] = summary['flg_NFappr']['sum'].cumsum()
    summary['cumu_NFappr_pcnt'] = summary['flg_NFappr']['sum'].cumsum()/summary['flg_NFappr']['sum'].sum()
    summary['cumu_fund'] = summary['flg_fund']['sum'].cumsum()
    summary['cumu_fund_pcnt'] = summary['flg_fund']['sum'].cumsum()/summary['flg_fund']['sum'].sum()
    summary['cumu_NFfund'] = summary['flg_NFfund']['sum'].cumsum()
    summary['cumu_NFfund_pcnt'] = summary['flg_NFfund']['sum'].cumsum()/summary['flg_NFfund']['sum'].sum()

    summary['large_funded'] = summary['large_fund']['sum'] 
    summary['cumu_large_funded'] = summary['large_fund']['sum'].cumsum()
    summary['cumu_large_funded_pcnt'] = summary['large_fund']['sum'].cumsum()/summary['large_fund']['sum'].sum()  
    summary['large_ags'] = summary['large_ags']['sum'] 
    summary['cumu_large_revenue'] = summary['large_ags']['sum'].cumsum()
    summary['cumu_large_revenue_pcnt'] = summary['large_ags']['sum'].cumsum()/summary['large_ags']['sum'].sum()   

    summary['resp_rate'] = (100*summary['flg_resp']['sum']/summary['flg_resp']['count']).round(2)
    summary['qual_rate'] = (100*summary['flg_qual']['sum']/summary['flg_resp']['count']).round(2)
    summary['sub_rate'] = (100*summary['flg_sub']['sum']/summary['flg_resp']['count']).round(2)
    summary['appr_rate'] = (100*summary['flg_appr']['sum']/summary['flg_resp']['count']).round(2)
    summary['NFappr_rate'] = (100*summary['flg_NFappr']['sum']/summary['flg_resp']['count']).round(2)
    summary['fund_rate'] = (100*summary['flg_fund']['sum']/summary['flg_resp']['count']).round(2)
    summary['NFfund_rate'] = (100*summary['flg_NFfund']['sum']/summary['flg_resp']['count']).round(2)
    
    summary['resp_qual_rate'] = (100*summary['flg_qual']['sum']/summary['flg_resp']['sum']).round(2)
    summary['resp_sub_rate'] = (100*summary['flg_sub']['sum']/summary['flg_resp']['sum']).round(2)
    summary['sub_appr_rate'] = (100*summary['flg_appr']['sum']/summary['flg_sub']['sum']).round(2)
    summary['sub_NFappr_rate'] = (100*summary['flg_NFappr']['sum']/summary['flg_sub']['sum']).round(2)
    summary['appr_fund_rate'] = (100*summary['flg_fund']['sum']/summary['flg_appr']['sum']).round(2)
    summary['appr_NFfund_rate'] = (100*summary['flg_NFfund']['sum']/summary['flg_appr']['sum']).round(2)
    
    summary['ROI_mail'] = (summary['fund_margin']['sum']/(0.41*summary['flg_resp']['count'])).round(2)
    summary['cumu_ROI_mail'] = (summary['fund_margin']['sum'].cumsum()/(0.41*summary['cumu_cnt'])).round(2)
    summary['ROI_business'] = (summary['fund_margin']['sum']/(0.41*summary['mail_cnt']['sum'])).round(2)
    summary['cumu_ROI_business'] = (summary['fund_margin']['sum'].cumsum()/(0.41*(summary['mail_cnt']['sum'].cumsum()))).round(2)

    summary['avg_funded'] = (summary['fund_amt']['sum']/summary['flg_fund']['sum']).round(0) 
    summary['avg_NFfunded'] = (summary['NFfund_amt']['sum']/summary['flg_NFfund']['sum']).round(0)   
    summary['cumu_fund_amt'] = summary['fund_amt']['sum'].cumsum()
    summary['cumu_fund_amt_pcnt'] = summary['fund_amt']['sum'].cumsum()/summary['fund_amt']['sum'].sum() 
    summary['cumu_NFfund_amt'] = summary['NFfund_amt']['sum'].cumsum()
    summary['cumu_NFfund_amt_pcnt'] = summary['NFfund_amt']['sum'].cumsum()/summary['NFfund_amt']['sum'].sum() 
    
    summary['good_bad_ratio_resp'] = summary['flg_resp']['sum']/(summary['flg_resp']['count']-summary['flg_resp']['sum'])
    summary['good_bad_ratio_qual'] = summary['flg_qual']['sum']/(summary['flg_resp']['count']-summary['flg_qual']['sum'])
    summary['good_bad_ratio_sub'] = summary['flg_sub']['sum']/(summary['flg_resp']['count']-summary['flg_sub']['sum'])
    summary['good_bad_ratio_appr'] = summary['flg_appr']['sum']/(summary['flg_resp']['count']-summary['flg_appr']['sum'])
    summary['good_bad_ratio_fund'] = summary['flg_fund']['sum']/(summary['flg_resp']['count']-summary['flg_fund']['sum'])
       
    
    #################################################
    # prepare measure list when not in full display
    #################################################
    target_short = target_label[4:]    
    list_measure_when_not_full = ['cumu_cnt',f'flg_{target_short}','resp_rate','sub_rate','appr_rate', 'fund_rate', f'cumu_{target_short}',f'cumu_{target_short}_pcnt',
                                  f'good_bad_ratio_{target_short}','ROI_mail','cumu_ROI_mail',
                                  'resp_qual_rate','resp_sub_rate','sub_appr_rate','sub_NFappr_rate','appr_fund_rate','appr_NFfund_rate','avg_funded', 'cumu_fund_pcnt', 'cumu_fund_amt_pcnt']
    
    # if any([c for c in df.columns if 'TIB' in c])==True:
    #     summary['avg_TIB'] = summary['TIB']['mean'].round(1)
    #     list_measure_when_not_full.append('avg_TIB')
    # if any([c for c in df.columns if 'gc_employees' in c])==True:
    #     summary['avg_gc_employees'] = summary['gc_employees']['mean'].round(1)
    #     list_measure_when_not_full.append('avg_gc_employees')
    # if any([c for c in df.columns if 'gc_sales' in c])==True:
    #     summary['avg_gc_sales_k'] = (summary['gc_sales']['mean']/1000).round(0)
    #     list_measure_when_not_full.append('avg_gc_sales_k')
        
    if any([c for c in df.columns if 'score_RESP' in c])==True:
        summary['min_score_RESP'] = summary['score_RESP']['min']
        summary['max_score_RESP'] = summary['score_RESP']['max']
        list_measure_when_not_full.append('min_score_RESP')
        list_measure_when_not_full.append('max_score_RESP')
    
    #################################################
    # output
    #################################################
    if full:
        return summary
    
    else:
        return summary[list_measure_when_not_full]

# COMMAND ----------

def create_roc(y_scalar, y_scalar_score):
    fpr, tpr, thresholds = metrics.roc_curve(y_scalar, y_scalar_score, pos_label=1)
    
    # calculate the g-mean for each threshold
    gmeans = np.sqrt(tpr * (1-fpr))
    # locate the index of the largest g-mean
    ix = np.argmax(gmeans)
    print('Best Threshold=%f, G-Mean=%.3f' % (thresholds[ix], gmeans[ix]))

    roc_auc = metrics.auc(fpr, tpr)
    
    fig, ax = plt.subplots(figsize=(6, 5))
    plt.title(' Receiver Operating Characteristic')
    ax.plot(fpr, tpr, 'b', label = 'AUC = %0.2f' % roc_auc)
    ax.scatter(fpr[ix], tpr[ix], marker='o')
    plt.legend(loc = 'lower right')
    plt.plot([0, 1], [0, 1],'r--')
    plt.xlim([0, 1])
    plt.ylim([0, 1])
    plt.ylabel('True Positive Rate')
    plt.xlabel('False Positive Rate')
    plt.show()

# COMMAND ----------

def report_performance(df, col_proba, target_label, report_full_metrics=True):
    # prepare data
    dff = df.copy()
    dff['bestmodel_score']  = dff[col_proba]
    dff['model_pred'] = 0
    dff.loc[dff['bestmodel_score'] >= 0.5, 'model_pred'] = 1
    
    if target_label != 'flg_appr':
        score_bins = np.arange(0,1.1,0.1)
        dff['bestmodel_scorebin'] = (pd.cut(dff['bestmodel_score'], bins=score_bins, right=False))
        dff['bestmodel_decile'] = (pd.qcut(dff['bestmodel_score'].rank(method='first'), q=10)).rank(method='dense', ascending=False)
    else:
        score_bins = np.arange(0.2,0.81,0.2)
        dff['bestmodel_scorebin'] = (pd.cut(dff['bestmodel_score'], bins=score_bins, right=False))
        dff['bestmodel_decile'] = (pd.qcut(dff['bestmodel_score'].rank(method='first'), q=5)).rank(method='dense', ascending=False)

    if dff['ref_score'].dropna().empty != True:
        dff['reference_scorebin'] = (pd.cut(dff['ref_score'], bins=score_bins, right=False))
        dff['reference_decile'] = (pd.qcut(dff['ref_score'].rank(method='first'), q=10)).rank(method='dense', ascending=False) 

    if 'custom_response' in dff.columns:
        dff['custom_response_decile'] = (pd.qcut(dff['custom_response'].rank(method='first'), q=10)).rank(method='dense', ascending=True)  
    if 'custom_funded' in dff.columns:
        dff['custom_funded_decile'] = (pd.qcut(dff['custom_funded'].rank(method='first'), q=10)).rank(method='dense', ascending=True)  
      
    
    dff['large_fund'] = 0
    dff.loc[dff['fund_amt'] >= 25000, 'large_fund'] = 1

    dff['large_ags'] = 0
    dff.loc[dff['lead_ags'] >= 250000, 'large_ags'] = 1
    
    # metric
    
    print(f"roc_auc_score: {metrics.roc_auc_score(dff[target_label], dff['bestmodel_score']).round(3)}")
    print(f"accuracy: {metrics.accuracy_score(dff[target_label], dff['model_pred']).round(3)}")
    print(f"precision: {metrics.precision_score(dff[target_label], dff['model_pred']).round(3)}")
    print(f"recall: {metrics.recall_score(dff[target_label], dff['model_pred']).round(3)}")
    print(f"f1_score: {metrics.f1_score(dff[target_label], dff['model_pred']).round(3)}")
    
    metrics_df = pd.DataFrame({'metrics':['accuracy', 'percision','recall', 'f1_score', 'roc_auc_score'], 'values':[metrics.accuracy_score(dff[target_label], dff['model_pred']).round(3), 
                                                                                                   metrics.precision_score(dff[target_label], dff['model_pred']).round(3),
                                                                                                   metrics.recall_score(dff[target_label], dff['model_pred']).round(3),
                                                                                                   metrics.f1_score(dff[target_label], dff['model_pred']).round(3), 
                                                                                                   metrics.roc_auc_score(dff[target_label], dff['model_pred']).round(3)]})
    
    # plot score separation
    g =  sns.FacetGrid(dff, hue = target_label, palette = ['red','green'])  
    g.map(sns.kdeplot, "bestmodel_score")
    plt.show()
    
    # roc curve
    print('best model ROC' + "="*10)
    _ = create_roc(dff[target_label], dff['bestmodel_score'])
    print('reference model ROC' + "="*10)
    _ = create_roc(dff[target_label], dff['ref_score'])
        
        
    # summary_tab
    # model set
    summary = gen_summary_tab(dff, 'bestmodel_scorebin', target_label, False, report_full_metrics)
    displayHTML(summary.to_html())
    
    summary = gen_summary_tab(dff, 'bestmodel_decile', target_label, True, report_full_metrics)
    displayHTML(summary.to_html())
    
    # reference scores
    if dff['ref_score'].dropna().empty != True:
        summary = gen_summary_tab(dff, 'reference_scorebin', target_label, False, report_full_metrics)
        displayHTML(summary.to_html())
        
        summary = gen_summary_tab(dff, 'reference_decile', target_label, True, report_full_metrics)
        displayHTML(summary.to_html())
        
        summary = gen_summary_tab(dff[dff['reference_decile'].isna()==False], 'reference_decile', target_label, True, report_full_metrics)
        displayHTML(summary.to_html())
        
    # dnb
    if (target_label == 'flg_resp') & ('custom_response' in dff.columns):
        summary = gen_summary_tab(dff, 'custom_response', target_label, True, report_full_metrics)
        displayHTML(summary.to_html())
        summary = gen_summary_tab(dff, 'custom_response_decile', target_label, True, report_full_metrics)
        displayHTML(summary.to_html())
        
    if (target_label != 'flg_resp') & ('custom_funded' in dff.columns):
        summary = gen_summary_tab(dff, 'custom_funded', target_label, True, report_full_metrics)
        displayHTML(summary.to_html())
        summary = gen_summary_tab(dff, 'custom_funded_decile', target_label, True, report_full_metrics)
        displayHTML(summary.to_html())
    
    
    # clean up to output the metrics
    output = metrics_df.T
    output.columns = output.iloc[0]
    output = output[1:] 
    
    return output

# COMMAND ----------

def performance_crosstab(dff, col1, col2, ascending=[True, True]):
    print('='*50 + 'count')
    tab = pd.crosstab(dff[col1], dff[col2])
    desc_values = tab.columns.sort_values(ascending=ascending[1])
    tab = tab[desc_values].sort_index(ascending=ascending[0])
    displayHTML_formated(tab, '{:,}')
    
    print('='*50 + 'row percentage')
    tab = pd.crosstab(dff[col1], dff[col2], normalize='index')*100
    desc_values = tab.columns.sort_values(ascending=ascending[1])
    tab = tab[desc_values].sort_index(ascending=ascending[0])
    displayHTML_formated_background_color(tab, '{:.1f}%')

    print('='*50 + 'column percentage')
    tab = pd.crosstab(dff[col1], dff[col2], normalize='columns')*100
    desc_values = tab.columns.sort_values(ascending=ascending[1])
    tab = tab[desc_values].sort_index(ascending=ascending[0])
    displayHTML_formated_background_color(tab, '{:.1f}%')

    groupby = gen_summary_tab(df=dff, var=[col1, col2], target_label='flg_resp', sort_ascending = True, full=True)
    
    # NOTE!!! Do NOT specify metric with 'cumu', the numbers won't be correct!!!
    # Because the overall sum will be overall sum instead of the margtin/subtotal by col1
    metric_list = ['resp_rate', 'resp_qual_rate', 'sub_rate', 'appr_rate', 'fund_rate', 'flg_fund', 'flg_NFfund', 'avg_funded', 'avg_NFfunded', 'ROI_mail']
    
    for metric in metric_list:
        print('='*50 + metric)
        tab = groupby.pivot_table(index=col1, columns=col2, values=metric, aggfunc='max')
        
        if df.columns.nlevels > 1:
            tab.columns = tab.columns.droplevel(0)

        desc_values = tab.columns.sort_values(ascending=ascending[1])
        tab = tab[desc_values].sort_index(ascending=ascending[0])
        
        if metric in ['avg_funded', 'avg_NFfunded']:
            fmt = "${:,.0f}"
        elif metric in ['ROI_mail']:
            fmt = "{:,.2f}"
        elif metric in ['cnt','flg_fund', 'flg_NFfund']:
            fmt = "{:,.0f}"
        elif metric in ['sub_rate', 'appr_rate', 'fund_rate']:
            fmt = '{:.2f}%'
        else:
            fmt = '{:.1f}%'
        
        if tab.empty == False:
            displayHTML_formated_background_color(tab, fmt)
        

# COMMAND ----------

def performance_crosstab_without_performance(dff, col1, col2, ascending=[True, True]):
    print('='*50 + 'count')
    tab = pd.crosstab(dff[col1], dff[col2])
    desc_values = tab.columns.sort_values(ascending=ascending[1])
    tab = tab[desc_values].sort_index(ascending=ascending[0])
    displayHTML_formated(tab, '{:,}')

    print('='*50 + 'column percentage')
    tab = pd.crosstab(dff[col1], dff[col2], normalize='columns')*100
    desc_values = tab.columns.sort_values(ascending=ascending[1])
    tab = tab[desc_values].sort_index(ascending=ascending[0])
    displayHTML_formated_background_color(tab, '{:.1f}%')
    
    print('='*50 + 'row percentage')
    tab = pd.crosstab(dff[col1], dff[col2], normalize='index')*100
    desc_values = tab.columns.sort_values(ascending=ascending[1])
    tab = tab[desc_values].sort_index(ascending=ascending[0])
    displayHTML_formated_background_color(tab, '{:.1f}%')
    
    print('='*50 + 'cell percentage')
    tab = pd.crosstab(dff[col1], dff[col2], normalize='all')*100
    desc_values = tab.columns.sort_values(ascending=ascending[1])
    tab = tab[desc_values].sort_index(ascending=ascending[0])
    print(tab)
    displayHTML_formated_background_color(tab, '{:.1f}%')