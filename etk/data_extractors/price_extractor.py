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

def extract_price(doc):
	digpe = DIGPriceExtractor()
	prices = digpe.extract(doc)

	
	if "price" not in prices:
		return None

	prices = prices['price']
	result = []
	for price in prices:
		ans = {}
		ans["value"] = price["price"]
		ans["metadata"] = {}
		ans["metadata"]["currency"] = price["price_unit"]
		tunit = price["time_unit"]
		# Converting all time units to minutes. Default - 60
		tunit_val = None
		if tunit in UNIT_TIME_HALF_HOUR:
			tunit_val = 30
		elif tunit in UNIT_TIME_HOUR:
			tunit_val = 60
		elif tunit in UNIT_TIME_MINUTE:
			tunit_val = tunit
		elif tunit in UNIT_TIME_SECOND:
			tunit_val = tunit/60
		else:
			#Default is 60
			tunit_val = 60

		ans["metadata"]["time_unit"] = tunit_val
		result.append(ans)

	return result




