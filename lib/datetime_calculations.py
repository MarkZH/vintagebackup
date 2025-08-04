"""Functions for calculations with dates and times."""

import datetime

from lib.argument_parser import CommandLineError


def parse_time_span_to_timepoint(
        time_span: str,
        now: datetime.datetime | None = None) -> datetime.datetime:
    """
    Parse a string representing a time span into a datetime representing a date that long ago.

    For example, if time_span is "6m", the result is a date six calendar months ago.

    :param time_span: A string consisting of a positive integer followed by a single letter: "d"
    for days, "w" for weeks, "m" for calendar months, and "y" for calendar years.
    :param now: The point from which to calculate the past point. If None, use
    datetime.datetime.now().
    """
    time_span = "".join(time_span.lower().split())
    try:
        number = int(time_span[:-1])
    except ValueError:
        raise CommandLineError(
            f"Invalid number in time span (must be a whole number): {time_span}") from None

    if number < 1:
        raise CommandLineError(f"Invalid number in time span (must be positive): {time_span}")

    letter = time_span[-1]
    now = now or datetime.datetime.now()
    match letter:
        case "d":
            return now - datetime.timedelta(days=number)
        case "w":
            return now - datetime.timedelta(weeks=number)
        case "m":
            new_date = months_ago(now, number)
            return datetime.datetime.combine(new_date, now.time())
        case "y":
            new_date = fix_end_of_month(now.year - number, now.month, now.day)
            return datetime.datetime.combine(new_date, now.time())
        case _:
            raise CommandLineError(f"Invalid time (valid units: {list("dwmy")}): {time_span}")


def months_ago(now: datetime.datetime | datetime.date, month_count: int) -> datetime.date:
    """
    Return a date that is a number of calendar months ago.

    The day of the month is not changed unless necessary to produce a valid date
    (see fix_end_of_month()).
    """
    new_month = now.month - (month_count % 12)
    new_year = now.year - (month_count // 12)
    if new_month < 1:
        new_month += 12
        new_year -= 1
    return fix_end_of_month(new_year, new_month, now.day)


def fix_end_of_month(year: int, month: int, day: int) -> datetime.date:
    """
    Replace a day past the end of the month (e.g., Feb. 31) with the last day of the same month.

    >>> fix_end_of_month(2023, 2, 31)
    datetime.date(2023, 2, 28)

    >>> fix_end_of_month(2024, 2, 31)
    datetime.date(2024, 2, 29)

    >>> fix_end_of_month(2025, 4, 31)
    datetime.date(2025, 4, 30)

    All other days are unaffected.

    >>> fix_end_of_month(2025, 5, 23)
    datetime.date(2025, 5, 23)
    """
    while True:
        try:
            return datetime.date(year, month, day)
        except ValueError:
            day -= 1
