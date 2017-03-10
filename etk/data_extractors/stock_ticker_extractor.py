import re

stock_ticker_pattern = '(?P<PreXChangeCode>[A-Z]{2,4}:(?![A-Z\d]+\.))?(?P<Stock>[A-Z]{2,4}|\d{1,3}(?=\.)|\d{4,})(?P<alt>-[A-Z])?(?P<PostXChangeCode>\.[A-Z]{2})?'

def extract_stock_tickers(text_string):
	stock_tickers = []
	for match in re.finditer(stock_ticker_pattern, text_string, flags=0):
		stock_ticker = {}
		
		stock_ticker['context'] = {'start' : match.start(), 'end' : match.end()}
		
		stock_ticker_symbol = ''
		for i in range(4):
			if match.group(i) != None:
				stock_ticker_symbol += match.group(i)
		stock_ticker['value'] = stock_ticker_symbol

		stock_tickers.append(stock_ticker)

	return stock_tickers