import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

def check_missing_values(df):
    """Returns a dataframe with missing value counts and percentages."""
    missing = df.isnull().sum()
    missing_percent = (df.isnull().sum() / len(df)) * 100
    df_missing = pd.DataFrame({'Missing Values': missing, 'Percentage': missing_percent})
    return df_missing[df_missing['Missing Values'] > 0].sort_values(by='Percentage', ascending=False)

def plot_correlation_heatmap(df, figsize=(10, 8)):
    """Plots a correlation heatmap for numeric columns."""
    plt.figure(figsize=figsize)
    numeric_df = df.select_dtypes(include=[np.number])
    corr = numeric_df.corr()
    sns.heatmap(corr, annot=True, cmap='coolwarm', fmt=".2f")
    plt.title('Correlation Heatmap')
    plt.show()

def plot_distribution(df, column, bins=30):
    """Plots the distribution of a numeric column."""
    plt.figure(figsize=(8, 6))
    sns.histplot(df[column], bins=bins, kde=True)
    plt.title(f'Distribution of {column}')
    plt.show()

def plot_categorical_counts(df, column):
    """Plots the counts of a categorical column."""
    plt.figure(figsize=(8, 6))
    sns.countplot(y=df[column], order=df[column].value_counts().index)
    plt.title(f'Counts of {column}')
    plt.show()

def get_basic_stats(df):
    """Returns basic statistics for the dataframe."""
    return df.describe(include='all')
