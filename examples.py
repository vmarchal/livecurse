import datetime as dt
import pandas as pd

from livecurse import LiveDataFrame

DA_URL = "http://mis.nyiso.com/public/csv/damlbmp/{:%Y%m%d}damlbmp_zone.csv"
RT_URL = "http://mis.nyiso.com/public/csv/rtlbmp/{:%Y%m%d}rtlbmp_zone.csv"


def ptid_to_initial(ptids):
    """
    Zones in NYISO have three identifiers: Letter, PTID and Name.
    This function seeks to return the letter identifier from the PTIDs
    :param ptids: series of the ptids of the data file
    :return: letters, associated with the ptids
    """
    letter_mapper = {
        p: chr(97 + i).upper()
        for i, p in enumerate(ptids.sort_values().unique())
    }
    return ptids.map(letter_mapper)


def get_data():
    today = dt.date.today() - dt.timedelta(1)
    urls = (url.format(today) for url in (DA_URL, RT_URL))
    # get raw dfs, pre-parse
    da, rt = (pd.read_csv(url,
                          names=['hour', 'ptid', 'lmp'],
                          usecols=[0, 2, 3],
                          index_col='hour',
                          parse_dates=['hour'],
                          header=0)
        for url in urls)
    # transform data to desired format
    da, rt = (
        df
        .assign(letter=lambda x: ptid_to_initial(x.ptid))
        .drop('ptid', axis=1)
        .pivot(columns='letter', values='lmp')
        for df in (da, rt)
    )
    # modify and sort column names; make spread df
    subset = 'AOMPNIK'
    da, rt, dr = (
        df
        .drop([c for c in df.columns if c not in subset], axis=1)
        .rename(columns={c: c+'.'+acr for c in df.columns})
        .sort_index(axis=1)
        for df, acr in zip((da, rt, da - rt), ('DA', 'RT', 'DR'))
    )

    return pd.concat([da, rt, dr], axis=1).sort_index(axis=1)

df = get_data()
formatters = {c: '{:.1f}'.format for c in df.columns}
index_formatter = '{:%m-%d %H}h'.format

live_df = LiveDataFrame(
    update_func=get_data,
    update_freq=30,
    formatters=formatters,
    index_formatter=index_formatter
)

high_dr = lambda x: 15 > x > 5
v_high_dr = lambda x: x >= 15
low_dr = lambda x: -15 < x < -5
v_low_dr = lambda x: x <= -15

for c in (col for col in df.columns if 'DR' in col):
    live_df.add_highlighters(c,
                             black_green=v_high_dr,
                             green_black=high_dr,
                             red_black=low_dr,
                             black_red=v_low_dr)

print(live_df._df_to_str(df, formatters, index_formatter))
print(live_df._df_to_formatters(df))
live_df.main()
