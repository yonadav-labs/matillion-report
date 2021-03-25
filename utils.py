def remove_footer(df):
    new_df = df.dropna(subset=[df.columns[1]])

    return new_df
