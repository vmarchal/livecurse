
import itertools
import time
import curses
from contextlib import contextmanager

import pandas as pd

_COLORS = ['black', 'blue', 'cyan', 'green', 'magenta',
           'red', 'white', 'yellow']

COLORS = {
    c: constant for c, constant in zip(
        [c for c in _COLORS],
        [getattr(curses, 'COLOR_' + c.upper()) for c in _COLORS]
    )
}

DEFAULT_FORMATTER = '{:.2f}'.format


@contextmanager
def print_attributes(stdscr, *args):
    for arg in args:
        stdscr.attron(arg)
    yield
    for arg in args:
        stdscr.attroff(arg)


class LiveDataFrame:
    """
    An object meant to present a pandas.DataFrame in curses, returned by update_func,
    whose values are formatted by formatters, that can be colored using
    functions using color_mappers
    """
    def __init__(self, update_func, update_freq, formatters,
                 index_formatter=None, default_color='white_black'):
        self.update_func, self.update_freq = update_func, update_freq
        self.formatters = formatters
        self.index_formatter = index_formatter
        self.default_pair = self._parse_color_kword(default_color)
        self._color_pairs = set()
        self._pair_to_pairnum = {self.default_pair: 1}
        self._col_color_mappers = dict()

    def add_highlighters(self, column, **highlighters):
        # Define functions returning a color pair if returned value of
        # original function is true
        def _make_function(func, color_pair):
            def new_func(x):
                if func(x):
                    return self._pair_to_pairnum[color_pair]
                return 1
            return new_func

        for kwd in highlighters.keys():
            pair = self._parse_color_kword(kwd)
            if pair not in self._pair_to_pairnum.keys():
                self._pair_to_pairnum.update(
                    {pair: len(self._pair_to_pairnum) + 1}
                )

        new_functions = [
            _make_function(f, self._parse_color_kword(k))
            for k, f in highlighters.items()
        ]
        self._col_color_mappers.update({column: new_functions})
        self._color_pairs = self._color_pairs.union({
            self._parse_color_kword(k) for k in highlighters.keys()
        })

    def main(self):
        curses.wrapper(self._draw_df)

    def _draw_df(self, stdscr):
        stdscr.clear()
        stdscr.refresh()

        # Start curses colors
        self._configure_colors()
        while True:
            start = time.time()
            height, width = stdscr.getmaxyx()
            # get text and formatter dataframes
            str_df, curse_format_df = self._make_dfs()
            min_size = sum(str_df[c].str.len().max() + 1
                           for c in str_df.columns)
            if min_size > width - 4:
                raise ValueError("Terminal too small to display data, please "
                                 "resize terminal\n"
                                 "Minimum width is {}".format(min_size + 4))
            self._update_screen(stdscr, height, width, str_df, curse_format_df)
            stdscr.refresh()
            end = time.time()
            if (end - start) < self.update_freq:
               time.sleep(end - start)

    def _update_screen(self, stdscr, height, width, str_df, cfmt_df):
        # get lengths of str_df contents, account for additionnal '|'
        lengths = [str_df[c].str.len().max() + 1 for c in str_df.columns]
        # get beginning x coordinates of
        begs = list(itertools.accumulate(lengths))
        start_x = start_y = 0
        for iy in range(len(str_df)):
            y = start_y + iy
            for ix in range(len(lengths)):
                txt = str_df.iloc[iy, ix]
                x = (start_x + 0 if ix == 0 else begs[ix-1])
                if (ix == 0) | (iy == 0):
                    args = (curses.A_BOLD, curses.color_pair(1))
                else:
                    args = (curses.color_pair(cfmt_df.iloc[iy-1, ix-1]),)
                with print_attributes(stdscr, *args):
                    stdscr.addstr(y, x, txt)
                stdscr.addstr(y, x + len(txt), '|')

    def _make_dfs(self):
        data = self.update_func()
        str_df = self._df_to_str(data,
                                 self.formatters,
                                 self.index_formatter)
        cfmt_df = self._df_to_formatters(data)
        return str_df, cfmt_df

    def _df_to_formatters(self, df):
        color_maps = self._col_color_mappers
        result = pd.DataFrame(data=1, index=df.index, columns=df.columns)
        for col, functions in color_maps.items():
            for func in functions:
                result.loc[result[col] == 1, col] = df.loc[result[col] == 1,
                                                           col].apply(func)
        return result

    def _configure_colors(self):
        curses.start_color()
        curses.init_pair(1, *self.default_pair)
        for p, num in self._pair_to_pairnum.items():
            curses.init_pair(num, *p)

    @staticmethod
    def _parse_color_kword(kword):
        foregrnd, backgrnd = kword.split('_')
        return COLORS[foregrnd], COLORS[backgrnd]

    @staticmethod
    def _df_to_str(df, formatters, index_formatter):
        def format_column(col, format_func):
            def formatter_func(df):
                return df[col].map(format_func)
            return formatter_func
        if not df.index.name:
            df.index.name = ' '
        columns_df = pd.DataFrame({c: [c] for c
                                   in [df.index.name, *df.columns]})
        formatter = {
                c: format_column(c, format_func)
                for c, format_func in formatters.items()
        }
        str_df = df.assign(**formatter)
        if index_formatter:
            str_df = (
                str_df
                .set_index(df.index.map(index_formatter))
                .reset_index()
            )
        full_df = pd.concat(
            [columns_df, str_df], ignore_index=True
        )
        # reorder columns so index is first
        full_df = full_df[[df.index.name, *df.columns]]
        full_df = full_df.assign(**{
            c: full_df[c].str.pad(full_df[c].str.len().max() + 1)
            for c in full_df.columns
        })
        full_df.to_csv('testest.csv')
        return full_df
