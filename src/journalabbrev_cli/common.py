import sys
from typing import *

from progressbar.bar import ProgressBar
from progressbar.widgets import WidgetBase


class FatalError(Exception):
    def __init__(self, exit_status: int = 1) -> None:
        super().__init__()
        self.exit_status = exit_status


def error(message: str) -> None:
    print("ERROR: " + message, file=sys.stderr, flush=True)


def warn(message: str) -> None:
    print("WARNING: " + message, file=sys.stderr, flush=True)


def info(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def progressbar_count_widgets(pbar: ProgressBar) -> Collection[WidgetBase]:
    import progressbar.widgets as widgets

    if pbar.max_value:
        return [
            widgets.Percentage(**pbar.widget_kwargs),
            " ",
            widgets.SimpleProgress(
                format="({value:,} of {max_value:,})",
                new_style=True,
                **pbar.widget_kwargs
            ),
            " ",
            widgets.Bar(**pbar.widget_kwargs),
            " ",
            widgets.Timer(**pbar.widget_kwargs),
        ]
    else:
        return [
            widgets.AnimatedMarker(**pbar.widget_kwargs),
            " ",
            widgets.BouncingBar(**pbar.widget_kwargs),
            " ",
            widgets.Counter(**pbar.widget_kwargs),
            " ",
            widgets.Timer(**pbar.widget_kwargs),
        ]
