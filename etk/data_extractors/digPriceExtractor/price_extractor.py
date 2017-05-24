from digpe import DIGPriceExtractor
from digpe.unit import *

UNIT_TIME_HALF_HOUR = [
    'half hour',
    'half hr',
    'half',
    'hlf hr',
    'hlf hour',
    'hf hr',
    'h hour',
    'h hr',
    'h h',
    'hhr',
    'hh',
    'hf'
]

UNIT_TIME_SECOND = [
    'ss',
    'second'
]

def cleaner(prices):
    new_prices = []
    for price in prices:
        val = int(price["value"])
        tunit = price["metadata"]["time_unit"]
        # For now, just remove these values
        if tunit in UNIT_TIME_SECOND or tunit in UNIT_TIME_MINUTE:
            continue
        if val % 5 == 0 and val < 1500 and val > 0:
            new_prices.append(price)

    return new_prices

def extract(text):
    digpe = DIGPriceExtractor()
    prices = digpe.extract(text)

    if "price" not in prices:
        return None

    prices = prices['price']
    result = []
    for price in prices:
        ans = dict()
        ans["value"] = int(price["price"])
        ans["metadata"] = {}
        ans["metadata"]["currency"] = price["price_unit"]
        tunit = price["time_unit"]

        # Converting all time units to minutes. Default - 60
        if tunit in UNIT_TIME_HALF_HOUR:
            tunit_val = "30"
        elif tunit in UNIT_TIME_HOUR:
            tunit_val = "60"
        elif " " in tunit:
            if tunit.split()[0].isdigit() is True:
                num = int(tunit.split()[0])
                time_unit = tunit.split()[1]
                multiplier = 1
                if time_unit in UNIT_TIME_HOUR:
                    multiplier = 60
                elif time_unit in UNIT_TIME_MINUTE:
                    multiplier = 1
                tunit_val = str(num * multiplier)
        else:
            # Default is 60
            tunit_val = "60"

        ans["metadata"]["time_unit"] = tunit_val
        result.append(ans)

    # Clean results
    new_prices = cleaner(result)
    return new_prices



