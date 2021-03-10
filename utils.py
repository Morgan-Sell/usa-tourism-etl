import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

from datetime import datetime



def plot_null_val_heatmap(df, plot_title, figsize):
    '''
    Identifies null values within a dataframe.
    
    Args:
        df (dataframe) : Dataset to evaluate null values.
        plot_title(str) : Title of heatmap.
        figsize (tuple) : Plot dimensions
    
    Return:
        None
    
    '''
    
    fig, ax = plt.subplots(figsize=figsize)
    ax = sns.heatmap(df.isnull(), yticklabels=False, cbar=False, cmap='viridis')
    ax.set_title(plot_title, fontsize=20)
    fig.tight_layout();

