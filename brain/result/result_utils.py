import pandas as pd
from utils.constants import (
    ALL_DF_COLUMNS,
    NUM_MAX_RESULT_FETCH,
    NUM_SUBSEQUENT_ROW_APPENDS,
)


def order_rows_from_df(df, cat_property, cat_category, num_rows=None, ordered=True):
    """
    to iloc a particular num of rows with given condition
    """

    if not num_rows:
        return df.iloc[(pd.Categorical(df[cat_property], categories=cat_category, ordered=ordered).argsort())]
    else:
        return df.iloc[(pd.Categorical(df[cat_property], categories=cat_category, ordered=ordered).argsort())].head(
            num_rows
        )


def make_continous_appends_for_df(dfs):
    """
    do continous appends for dfs
    """

    rows_appended = 0
    dfs = [df for df in dfs if df is not None]
    dftemp = pd.DataFrame(columns=ALL_DF_COLUMNS)
    while rows_appended <= NUM_MAX_RESULT_FETCH:
        for df in dfs:
            dftemp = dftemp.append(
                df.iloc[rows_appended : rows_appended + NUM_SUBSEQUENT_ROW_APPENDS],
                ignore_index=True,
            )
        rows_appended += NUM_SUBSEQUENT_ROW_APPENDS
    return dftemp
