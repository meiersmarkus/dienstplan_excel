"""Deutsche Feiertage (Hamburg) – wird einmal initialisiert und überall geteilt."""

import datetime
from typing import Optional, Tuple

import holidays
from dateutil.easter import easter


class GermanHolidays:
    """Feiertags-Prüfung für Hamburg inkl. Sondertage.

    Verwendung:
        feiertage = GermanHolidays()
        is_free, name = feiertage.is_holiday_or_weekend(datetime.date(2025, 12, 25))
    """

    def __init__(self, extra_years_range: int = 1):
        current_year = datetime.date.today().year
        years = list(range(current_year - extra_years_range, current_year + extra_years_range + 1))

        self._holidays = holidays.Germany(years=years, observed=False, prov="HH", language="de")

        # Zusätzliche Tage, die nicht im Standard-Paket sind
        for year in years:
            self._holidays[datetime.date(year, 10, 31)] = "Reformationstag"
            self._holidays[datetime.date(year, 12, 24)] = "Heiligabend"
            self._holidays[datetime.date(year, 12, 31)] = "Silvester"
            easter_date = easter(year)
            self._holidays[easter_date] = "Ostersonntag"
            self._holidays[easter_date + datetime.timedelta(days=49)] = "Pfingstsonntag"

    def is_holiday_or_weekend(self, datum: datetime.date) -> Tuple[bool, Optional[str]]:
        """Prüft ob ein Datum ein Feiertag oder Wochenende ist.

        Returns:
            (True, "Feiertags-/Wochenendname") oder (False, None)
        """
        if isinstance(datum, datetime.datetime):
            datum = datum.date()

        if datum in self._holidays:
            return True, self._holidays.get(datum)
        if datum.weekday() >= 5:
            return True, "Samstag" if datum.weekday() == 5 else "Sonntag"
        return False, None
