from datetime import date, timedelta
from typing import List, Optional

def get_iso_week_mondays(year: int, 
                         min_date: Optional[date] = None, 
                         max_date: Optional[date] = None) -> List[date]:
    """
    Generate all ISO week Monday dates for a given year.

    Args:
        year (int): Year to get ISO Mondays for.
        min_date (Optional[date]): Optional filter for minimum date.
        max_date (Optional[date]): Optional filter for maximum date.

    Returns:
        List[date]: List of datetime.date objects (all Mondays of ISO weeks).
    """
    d = date(year, 1, 4)
    d -= timedelta(days=d.weekday())

    mondays = []
    while d.year <= year or (d.year == year + 1 and d.isocalendar()[1] == 1):
        if (
            d.isocalendar()[0] == year and
            (min_date is None or d >= min_date) and
            (max_date is None or d <= max_date)
        ):
            mondays.append(d)
        d += timedelta(weeks=1)

    return mondays