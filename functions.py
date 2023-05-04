import matplotlib.pyplot as plt
from pylatex import Document, PageStyle, Head, MiniPage, Foot, LargeText, \
    MediumText, SmallText, LineBreak, simple_page_number, Section, \
    Subsection, TextBlock, HugeText, VerticalSpace, HorizontalSpace, \
    Tabular, Math, TikZ, Axis, Plot, Figure, Matrix, Alignat, SubFigure, \
    NewPage, Command
from pylatex.utils import bold, italic, NoEscape
import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter, StrMethodFormatter
import matplotlib.font_manager
import CONSTAINTS as c
from util import *


#########################################################################################################################
#########################################################################################################################
##   IMPORT DATA
#########################################################################################################################
#########################################################################################################################

def trailing_90_days_graph(df_input, date_field, unique_field, file_name, brand):
    # Use the newly integrated Roboto font family for all text.
    plt.rc('font', family='Rock Sans')
    
    fig = plt.figure(figsize=(3,3))
    ax= fig.add_axes([0.1,0.1,1,0.5])
    #  fig, ax = plt.subplots()
    
    ax.bar(df_input[date_field], df_input[unique_field], width=0.8, edgecolor='white')
        
    ax.autoscale(enable=True, axis='x', tight=True)
   
    plt.xticks(rotation=35, ha='right', fontsize=12, weight='bold')
    plt.yticks(fontsize=12, weight='bold')
    
    # after plotting the data, format the labels
    current_values = plt.gca().get_yticks()
    if max(list(df_input[unique_field])) <=10:
        ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.2f}'))
    else:
        ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
    
    # Remove every other x axis label
    for label in ax.xaxis.get_ticklabels()[1::2]:
        label.set_visible(False)
    
    # Make layout of graph tighter / smaller
    plt.tight_layout()
    
    plt.savefig(c.output_dir + brand + '/' + file_name, bbox_inches='tight', dpi=300)

    return ax


def trailing_90_days_horizontal_graph(df_input, y_field, unique_field, file_name, brand):
    
    variable_column = list(df_input[y_field])

    temp_list = []
    for j in variable_column:
        split_list = j.split(' ')
        temp_string = ''
        variable_len = 0
        line = 0
        for i in split_list:
            if line < 2:
                if len(temp_string) > 0:
                    temp_string = temp_string + ' ' + i
                    variable_len = variable_len + len(i) + 1
                else:
                    temp_string = i
                    variable_len = len(i)

                if variable_len > 40:
                    line = line + 1
                    if line == 1: 
                        temp_string = temp_string + '\n'
                        variable_len = 0
                    else: 
                        temp_string = temp_string + '...'

        temp_list.append(temp_string)
    df_input[y_field] = temp_list
    
    # Use the newly integrated Roboto font family for all text.
    plt.rc('font', family='Rock Sans')
    
    #     fig = plt.figure(figsize=(3,3))
    #     ax= fig.add_axes([0.1,0.1,1,0.5])
    fig, ax = plt.subplots()

    plt.barh(df_input[y_field],df_input[unique_field], edgecolor='white')

    ax.autoscale(enable=True, axis='x', tight=True)

    plt.xticks(rotation=35, ha='right', fontsize=16, weight='bold')
    plt.yticks(fontsize=16, weight='bold')

    # after plotting the data, format the labels
    current_values = plt.gca().get_xticks()
    ax.xaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
    
    plt.savefig(c.output_dir + brand + '/' + file_name, bbox_inches='tight', dpi=300)

    return ax


def stacking_graph(df_input, date_level, graph_title, legend, graph_file_name, brand):

    # Use the newly integrated Roboto font family for all text.
    plt.rc('font', family='Rock Sans')
    
    fig, ax = plt.subplots()

    # create data
    if date_level == 'Month':
        date_temp = list(df_input['date_month'])
    if date_level == 'Week':
        date_temp = list(df_input['week'])
    y1 = df_input[legend[0]].to_numpy()
    y2 = df_input[legend[1]].to_numpy()
    if graph_title == 'Visit Count by Referral Sources':
        y3 = df_input[legend[2]].to_numpy()
        y4 = df_input[legend[3]].to_numpy()


    # plot bars in stack manner
    plt.bar(date_temp, y1, color='r')
    plt.bar(date_temp, y2, bottom=y1, color='b')
    if graph_title == 'Visit Count by Referral Sources':
        plt.bar(date_temp, y3, bottom=y1+y2, color='y')
        plt.bar(date_temp, y4, bottom=y1+y2+y3, color='g')
        
    plt.xticks(rotation=35, ha='right', fontsize=16, weight='bold')
    plt.yticks(fontsize=16, weight='bold')

    # after plotting the data, format the labels
    current_values = plt.gca().get_yticks()
    plt.gca().set_yticklabels(['{:,.0f}'.format(x) for x in current_values])
    
    # Remove every other x-axis label
    if date_level == 'Week':
        xticks = plt.gca().xaxis.get_major_ticks()
        for i in range(len(xticks)):
            if i % 2 != 0:
                xticks[i].set_visible(False)
    
    # add legends and set its box position
    legend_properties = {'weight':'bold', 'size': 14}
    plt.legend(legend, bbox_to_anchor=(1.05, 0.6), prop=legend_properties)
                
    plt.tight_layout()

    plt.savefig(c.output_dir + brand + '/' + graph_file_name, bbox_inches='tight', dpi=300)

    return plt

def trailing_90_days_graph_with_benchmark(df_input, date_field, unique_field_name,
                                          unique_field, benchmark_field, file_name, brand):
    # Use the newly integrated Roboto font family for all text.
    plt.rc('font', family='Rock Sans')

    fig = plt.figure(figsize=(3,3))
    ax= fig.add_axes([0.1,0.1,1,0.5])
#     fig, ax = plt.subplots()
    
    plt.plot(df_input[date_field], df_input[benchmark_field], "-b", label='Benchmark')
    plt.bar(df_input[date_field], df_input[unique_field], width=0.8, edgecolor='white', label=unique_field_name)

    plt.xticks(rotation=35, ha='right', fontsize=12, weight='bold')
    plt.yticks(fontsize=12, weight='bold')
    
    # after plotting the data, format the labels
    current_values = plt.gca().get_yticks()
    if max(list(df_input[unique_field])) <=10:
        plt.gca().set_yticklabels(['{:,.2f}'.format(x) for x in current_values])
    else:
        plt.gca().set_yticklabels(['{:,.0f}'.format(x) for x in current_values])
    
    # add legends and set its box position
    plt.legend(loc='upper left', prop={'weight':'bold', 'size': 12})
    
    # Remove every other x axis label
    for label in ax.xaxis.get_ticklabels()[1::2]:
        label.set_visible(False)
        
    # Make layout of graph tighter / smaller
    plt.tight_layout()
    
    plt.savefig(c.output_dir + brand + '/' + file_name, bbox_inches='tight', dpi=300)

    return plt


def stacking_percentage_helper(legend_list, df_input):
    for i in legend_list:
        df_input[i] = (df_input[i]/df_input['total']) * 100
        df_input[i] = df_input[i].astype(float)

    df_input = df_input.reset_index()
    df_input = df_input.drop(['total'], axis=1)
    
    return df_input

def stacking_percentage_table(df_input, legend_variable, metric_variable, legend_list, ninety_days_ago_input):
    # Filter and remove columns
    df_temp = df_input[df_input['page_name_filter']=='Keep']
    df_temp = df_temp[['week', legend_variable, metric_variable]]
    
    #Filter 90 days
    df_temp = df_temp[df_temp['week']>=ninety_days_ago_input]

    # Calc Visit count
    df_temp = df_temp.groupby(['week', legend_variable])[metric_variable].nunique()
    df_temp = df_temp.reset_index()
    df_temp = df_temp.sort_values(by=['week'])
    
    # Pivot the table
    df_temp2 = df_temp.pivot_table(metric_variable, ['week'], legend_variable)

    # Find Percentage of total
    df_temp2['total'] = df_temp2[legend_list].sum(axis=1)
    df_temp3 = stacking_percentage_helper(legend_list, df_temp2)
    
    df_temp3['week'] = df_temp3['week'].dt.strftime('%b %d')
    
    return df_temp3


def create_final_dataframe(df_final, final_table_column, title_value, week_value, week_over_week, 
                           mtd_value, month_over_month, graph_png):
    details = {
        final_table_column[0] : [title_value],
        final_table_column[1] : [week_value],
        final_table_column[2] : [week_over_week],
        final_table_column[3] : [mtd_value],
        final_table_column[4] : [month_over_month],
        final_table_column[5] : [graph_png]
    }
    df_temp = pd.DataFrame(details)

    df_final = df_final.append(df_temp)
    
    df_final.reset_index(inplace=True, drop=True)

    return df_final


def metric_calc(calc, metric, df_temp_week, df_temp_previous_week, df_temp_month, df_temp_previous_month):
    if calc == 'sum':
        # Current Week
        metric_week = df_temp_week[metric].sum()
        
        # Week over Week
        metric_previous_week = df_temp_previous_week[metric].sum()
        metric_week_over_week = (metric_week - metric_previous_week)/metric_previous_week
        
        # MTD
        metric_mtd = df_temp_month[metric].sum()
        
        # Month over Month
        metric_previous_month = df_temp_previous_month[metric].sum()
        metric_month_over_month = (metric_mtd - metric_previous_month)/metric_previous_month
        
    if calc == 'count_distinct':
        # Current Week
        metric_week = df_temp_week[metric].nunique()
        
        # Week over Week
        metric_previous_week = df_temp_previous_week[metric].nunique()
        metric_week_over_week = (metric_week - metric_previous_week)/metric_previous_week
        
        # MTD
        metric_mtd = df_temp_month[metric].nunique()
        
        # Month over Month
        metric_previous_month = df_temp_previous_month[metric].nunique()
        metric_month_over_month = (metric_mtd - metric_previous_month)/metric_previous_month

    return metric_week, metric_week_over_week, metric_mtd, metric_month_over_month, metric_previous_week, metric_previous_month


def published_volume_filter(df_input):
    
    df_output = df_input[df_input['user_server'].isin(['www.nbc.com', 'www.usanetwork.com', 'www.bravotv.com', 
                                                       'www.syfy.com', 'www.oxygen.com'])]
    df_output = df_output[df_output['content_type'].isin(['Blog Post', 'Amp'])]
    
    return df_output


def referral_highlights_filter(df_input, referral_source):
    df_output = df_input[df_input['page_name_filter']=='Keep']
    df_output = df_output[df_output['referral_source']==referral_source]
    
    return df_output


def top_blog_graph_filters(df_input, referral_source, ninety_days_ago):
    df_output = df_input[df_input['page_name_filter']=='Keep']
    df_output = df_output[~df_output['pagename'].isin(['Identity: Link MVPD', 'Identity: Sign-in', 
                                                       'Identity: Sign-up', 'Identity: Signed Out', 
                                                       'Identity: Unlink MVPD', 'Identity: User Profile Completed'])]
    df_output = df_output[df_output['content_type'].isin(['Blog Post', 'Amp'])]
    df_output = df_output[df_output['referral_source']==referral_source]
    df_output = df_output[df_output['week']>=ninety_days_ago]
    df_output = df_output[['visits_calc_id', 'pagename']]
    df_output = df_output.groupby(['pagename'])['visits_calc_id'].nunique()
    df_output = df_output.reset_index()
    df_output = df_output.sort_values(by=['visits_calc_id'], ascending=False)
    df_output = df_output.head(5)
    df_output = df_output.sort_values(by=['visits_calc_id'])
    
    return df_output



def segment_starts_per_visitor(metric_week, metric_mtd, ninety_days_ago_input, df_week_input, 
                               df_video_previous_week_input, df_month_input, df_video_previous_month_input, 
                               df_video_input, metric):
    
    # Current Week
    visitor_week = df_week_input['visitor_id_calc'].nunique()
    metric_per_visitor_week = metric_week / visitor_week

    # Week over Week
    visitor_previous_week = df_video_previous_week_input['visitor_id_calc'].nunique()
    metric_previous_week = df_video_previous_week_input[metric].sum()
    metric_per_visitor_previous_week = metric_previous_week / visitor_previous_week
    metric_per_visitor_week_over_week = (metric_per_visitor_week - metric_per_visitor_previous_week)/\
        metric_per_visitor_previous_week

    # MTD
    visitor_mtd = df_month_input['visitor_id_calc'].nunique()
    metric_per_visitor_mtd = metric_mtd / visitor_mtd

    # Month over Month
    visitor_previous_month = df_video_previous_month_input['visitor_id_calc'].nunique()
    metric_previous_month = df_video_previous_month_input[metric].sum()
    metric_per_visitor_previous_month = metric_previous_month / visitor_previous_month
    metric_per_visitor_month_over_month = (metric_per_visitor_mtd - metric_per_visitor_previous_month)/\
        metric_per_visitor_previous_month

    # Trailing 90 Days
    df_temp1 = df_video_input[df_video_input['week']>=ninety_days_ago_input]
    df_graph_output = df_temp1.groupby('week').agg({metric: 'sum', 'visitor_id_calc': 'nunique'}).reset_index()

    df_graph_output[metric + '_per_visitor'] = df_graph_output[metric] / df_graph_output['visitor_id_calc']

    df_graph_output = df_graph_output.drop([metric, 'visitor_id_calc'], axis=1)
    df_graph_output['week'] = df_graph_output['week'].dt.strftime('%b %d')

    return metric_per_visitor_week, metric_per_visitor_week_over_week, metric_per_visitor_mtd, \
        metric_per_visitor_month_over_month, df_graph_output



def trailing_90_days_graph_table(df_input, df_benchmark_input, ninety_days_ago_input, week_value, month_value, unique_value, benchmark_value):
    
    df_temp = df_input[df_input[week_value]>=ninety_days_ago_input]
    df_temp_unique = df_temp.groupby([week_value])[unique_value].nunique()
    df_temp_unique = df_temp_unique.reset_index()

    # Get Benchmarks in terms of Weeks
    df_temp2 = df_temp[[month_value, week_value]]
    df_temp2 = df_temp2.rename(columns={month_value: 'date_month'})
    df_temp2 = df_temp2.drop_duplicates()
    
    df_benchmarks_temp = df_benchmark_input[['date_month', benchmark_value]]
    df_benchmarks_temp = df_benchmarks_temp.merge(df_temp2, on='date_month', how='left')
    df_benchmarks_temp = df_benchmarks_temp[df_benchmarks_temp[week_value]>=ninety_days_ago_input]
    df_benchmarks_temp = df_benchmarks_temp[[week_value, benchmark_value]]
    df_benchmarks_temp = df_benchmarks_temp.drop_duplicates()

    # Combine
    df_output = df_temp_unique.merge(df_benchmarks_temp, on=week_value, how='left')
    df_output[week_value] = df_output[week_value].dt.strftime('%b %d')
    
    return df_output


def switch_function(switch_input, metric_1, metric_2):
    if switch_input == 'no':
        output = metric_1 / metric_2
    if switch_input == 'yes':
        output = metric_2 / metric_1
    
    return output


def metric_per_visit(metric, visits_week_input, visits_previous_week_input, visits_mtd_input,
                     visits_previous_month_input, df_week_input, df_previous_week_input,
                     df_month_input, df_previous_month_input, switch):

    # Current Week
    metric_week = df_week_input[metric].sum()
    metric_per_visit_week = switch_function(switch, metric_week, visits_week_input)

    # Week over Week
    metric_previous_week = df_previous_week_input[metric].sum()
    metric_per_visit_previous_week = switch_function(switch, metric_previous_week, visits_previous_week_input)
    metric_per_visit_week_over_week = (metric_per_visit_week - metric_per_visit_previous_week)/\
                                        metric_per_visit_previous_week

    # MTD
    metric_mtd = df_month_input[metric].sum()
    metric_per_visit_mtd = switch_function(switch, metric_mtd, visits_mtd_input)

    # Month over Month
    metric_previous_month = df_previous_month_input[metric].sum()
    metric_per_visit_previous_month = switch_function(switch, metric_previous_month, visits_previous_month_input)
    metric_per_visit_month_over_month = (metric_per_visit_mtd - metric_per_visit_previous_month)/\
                                            metric_per_visit_previous_month
 
    
    return metric_per_visit_week, metric_per_visit_week_over_week, metric_per_visit_mtd, \
             metric_per_visit_month_over_month


def visit_count_per_post(visits_by_referral_week_input, visits_by_referral_previous_week_input, 
                         visits_by_referral_mtd_input, visits_by_referral_previous_month_input,
                         df_week_temp_input, df_previous_week_temp_input, df_month_temp_input):
    # Current Week
    post_by_referral_week = df_week_temp_input['content_id'].nunique()
    visits_per_post_by_referral_week = visits_by_referral_week_input / post_by_referral_week

    # Week over Week
    post_by_referral_previous_week = df_previous_week_temp_input['content_id'].nunique()
    visits_per_post_by_referral_previous_week = visits_by_referral_previous_week_input / \
                                                    post_by_referral_previous_week
    visits_per_post_by_referral_week_over_week = (visits_per_post_by_referral_week - \
                                                    visits_per_post_by_referral_previous_week)/\
                                                    visits_per_post_by_referral_previous_week

    # MTD
    post_by_referral_mtd = df_month_temp_input['content_id'].nunique()
    visits_per_post_by_referral_mtd = visits_by_referral_mtd_input / post_by_referral_mtd

    # Month over Month
    post_by_referral_previous_month = df_previous_week_temp_input['content_id'].nunique()
    visits_per_post_by_referral_previous_month = visits_by_referral_previous_month_input / \
                                                    post_by_referral_previous_month
    visits_per_post_by_referral_month_over_month = (visits_per_post_by_referral_mtd - \
                                                        visits_per_post_by_referral_previous_month)/\
                                                        visits_per_post_by_referral_previous_month

    return visits_per_post_by_referral_week, visits_per_post_by_referral_week_over_week, \
            visits_per_post_by_referral_mtd, visits_per_post_by_referral_month_over_month



#########################################################################################################################
#########################################################################################################################
##   PDF
#########################################################################################################################
#########################################################################################################################


#### PAGE 1

def create_card_mtd_page_1(section_name, metric_week, week_over_week, mtd, month_over_month, graph, doc):

    with doc.create(MiniPage(width=NoEscape(r"0.4\linewidth"))):
        with doc.create(Section(section_name + ': ' + str(metric_week) + ' (WoW ' + str(week_over_week) + ')', 
                                numbering= False)):
            with doc.create(MiniPage(width=NoEscape('140px'))):
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), align='l', pos='t')):
                    doc.append(bold(' MTD: '))
                    doc.append(mtd)
                doc.append(LineBreak())
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), height=NoEscape(r"0.9\textwidth"),
                                     align='l', pos='t')):
                    doc.append(bold(' MoM: '))
                    doc.append(month_over_month)

            with doc.create(
                SubFigure(position='c', width=NoEscape('210px'))) as right_imagesRow1:
                right_imagesRow1.add_image(graph, width=NoEscape(r'0.95\linewidth'))
    
    return doc

def create_card_mtd_month_projected(section_name, metric_week, week_over_week, mtd, month_over_month, pacing, graph, doc):
    with doc.create(MiniPage(width=NoEscape(r"0.4\linewidth"))):
        with doc.create(Section(section_name + ': ' + str(metric_week)+ ' (WoW ' + str(week_over_week) + ')',
                               numbering= False)):
            with doc.create(MiniPage(width=NoEscape('140px'))):
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), align='l', pos='t')):
                    doc.append(bold(' MTD: '))
                    doc.append(mtd)
                doc.append(LineBreak())
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"),
                                     align='l', pos='t')):
                    doc.append(bold(' MoM: '))
                    doc.append(month_over_month)
                doc.append(LineBreak())
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), height=NoEscape(r"0.8\textwidth"),
                                     align='l', pos='t')):
                    doc.append(bold(' Pacing to Goal: '))
                    doc.append(pacing)

            with doc.create(
                SubFigure(position='c', width=NoEscape('210px'))) as right_imagesRow1:
                right_imagesRow1.add_image(graph, width=NoEscape(r'0.95\linewidth'))
    
    return doc

def create_card_rolling(section_name, metric_week, week_over_week, graph, doc):
    with doc.create(MiniPage(width=NoEscape(r"0.4\linewidth"))):
        with doc.create(Section(section_name + ': ' + str(metric_week) + ' (WoW ' + str(week_over_week) + ')',
                                numbering= False)):
            with doc.create(
                SubFigure(position='r', width=NoEscape('210px'))) as right_imagesRow1:
                right_imagesRow1.add_image(graph, width=NoEscape(r'0.95\linewidth'))
    
    return doc



#### PAGE 2 and 3


def create_card_mtd(section_name, metric_week, week_over_week, mtd, month_over_month, graph, doc):
    with doc.create(MiniPage(width=NoEscape(r"0.5\linewidth"))):
        with doc.create(Section(
            section_name + ': ' + str(metric_week) + ' (WoW ' + str(week_over_week) + ')', numbering= False)):
            with doc.create(MiniPage(width=NoEscape('140px'))):
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), align='l', pos='t')):
                    doc.append(bold(' MTD: '))
                    doc.append(mtd)
                doc.append(LineBreak())
                with doc.create(MiniPage(width=NoEscape(r"0.98\textwidth"), height=NoEscape(r"0.9\textwidth"),
                                     align='l', pos='t')):
                    doc.append(bold(' MoM: '))
                    doc.append(month_over_month)

            with doc.create(
                SubFigure(position='c', width=NoEscape('210px'))) as right_imagesRow1:
                right_imagesRow1.add_image(graph, width=NoEscape(r'0.95\linewidth'))
    
    return doc


def create_card_no_metrics(section_name, graph, doc):
    with doc.create(MiniPage(width=NoEscape(r"0.5\linewidth"))):
        with doc.create(Section(section_name, numbering= False)):
            with doc.create(
                SubFigure(position='c', width=NoEscape(r"1\textwidth")
                         )) as right_imagesRow1:
                right_imagesRow1.add_image(graph, width=NoEscape(r'1\linewidth'))
    
    return doc


def create_card_no_metrics_stacked_graph(section_name, graph, doc):
    with doc.create(MiniPage(width=NoEscape(r"0.5\linewidth"))):
        with doc.create(Section(section_name, numbering= False)):
            with doc.create(
                SubFigure(position='c', width=NoEscape('190px')
                         )) as right_imagesRow1:
                right_imagesRow1.add_image(graph, width=NoEscape(r'1\linewidth'))
    
    return doc