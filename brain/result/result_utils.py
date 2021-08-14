import logger
import pandas as pd
from utils.constants import (
    ALL_DF_COLUMNS,
    NUM_MAX_RESULT_FETCH,
    NUM_SUBSEQUENT_ROW_APPENDS,
)

log = logger.get_logger(__name__)


def order_rows_from_df(df, cat_property, cat_category, num_rows=None, ordered=True):
    """
    to iloc a particular num of rows with given condition
    """

    log.debug(
        f"Ordering from df: {len(df.index.values)} for property: {cat_property} and category: {cat_category}, with num rows: {num_rows}, and ordered is: {ordered}"
    )
    if not num_rows:
        df_to_return = df.iloc[(pd.Categorical(df[cat_property], categories=cat_category, ordered=ordered).argsort())]
        log.debug(f"Returing df of length: {len(df_to_return.index.values)}")
        return df_to_return
    else:
        df_to_return = df.iloc[
            (pd.Categorical(df[cat_property], categories=cat_category, ordered=ordered).argsort())
        ].head(num_rows)
        log.debug(f"Returing df of length: {len(df_to_return.index.values)}")
        return df_to_return


def make_continous_appends_for_df(dfs):
    """
    do continous appends for dfs
    """

    log.debug(f"Making cont. appends for dfs: {len(dfs)}")
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
    log.debug(f"Returning cont. appends df of length: {len(dftemp)}")
    return dftemp
