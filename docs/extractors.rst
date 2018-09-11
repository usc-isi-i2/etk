Extractors
==========

.. automodule:: etk.extractors.bitcoin_address_extractor
    :members:
    :special-members:

.. automodule:: etk.extractors.cryptographic_hash_extractor
    :members:
    :special-members:

.. automodule:: etk.extractors.cve_extractor
    :members:
    :special-members:

.. automodule:: etk.extractors.date_extractor
    :members:
    :special-members:

    .. method:: extractextract(self, text: str = None,
                extract_first_date_only: bool = False,
                additional_formats: List[str] = list(),
                use_default_formats: bool = False,
                ignore_dates_before: datetime.datetime = None,
                ignore_dates_after: datetime.datetime = None,
                detect_relative_dates: bool = False,
                relative_base: datetime.datetime = None,
                preferred_date_order: str = "MDY",
                prefer_language_date_order: bool = True,
                timezone: str = None,
                to_timezone: str = None,
                return_as_timezone_aware: bool = True,
                prefer_day_of_month: str = "first",
                prefer_dates_from: str = "current",
                date_value_resolution: DateResolution = DateResolution.DAY,
                )