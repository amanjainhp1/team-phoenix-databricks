# import libraries
from cycler import cycler
from IPython.display import Image, HTML, display
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from pylab import text


def mpl_ts(df, format_dict):
    """
    paired down helper method;
    df must have 3 cols: cal_date, variable, float
    """
    fix, ax = plt.subplots()
    for key, grp in df.groupby(['variable']):
        ax = grp.plot(ax=ax, kind='line', label=key)

    for key, value in format_dict.items():

        # format ticks
        if key == 'ticker':
            ax.get_yaxis().set_major_formatter(
                mpl.ticker.FuncFormatter(lambda x, p: format(int(x), ','))
            )

        # create rectangle
        if key == 'axvspan_list':
            ax.axvspan(
                pd.Timestamp(value[0]),
                pd.Timestamp(value[1]),
                label=value[2],
                color="green",
                alpha=0.3
            )

        # create legend
        if key == 'legend_list':
            L = plt.legend()
            for idx, l in enumerate(value):
                L.get_texts()[idx].set_text(l)

        # titles
        if key == 'title_list':
            plt.title(value[0], fontsize=20)
            plt.xlabel(value[1], fontsize=16)
            plt.ylabel(value[2], fontsize=16)

    plt.grid()

    try:
        plt.savefig(format_dict['viz_path'], format='png')
    except KeyError:
        pass

    plt.show()


def set_background(color):
    script = (
        "var cell = this.closest('.jp-CodeCell');"
        "var editor = cell.querySelector('.jp-Editor');"
        "editor.style.background='{}';"
        "this.parentNode.removeChild(this)"
    ).format(color)

    display(HTML('<img src onerror="{}" style="display:none">'.format(script)))