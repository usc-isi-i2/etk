
def wrap_value_with_context(value, start, end):
    return {'value': value,
            'context': {'start': start,
                        'end': end
                        }
            }


def extract(tokens_url, country_code_dict):
    results = list()
    # Get country codes from url
    for token in tokens_url:
        if token in country_code_dict:
            # Check if its actually a country code in the orig url
            # pos = url.find('.' + token)
            # print token
            # print pos
            # if url[pos - 3:pos] in ['.co', '.ac'] or url[pos - 4:pos] in ['.org', '.com', '.edu', '.gov']:
            #     ann_countries.append(self.country_code_dict[token])

            index = tokens_url.index(token)
            if index - 1 >= 0:
                prev = index - 1
            else:
                prev = 0
            if index + 1 > len(tokens_url):
                nex = len(tokens_url)
            else:
                nex = index + 1
            valid_country = False
            if tokens_url[prev] == '.':
                if nex == len(tokens_url):
                    valid_country = True
                elif tokens_url[nex] == '/':
                    valid_country = True
            if valid_country:
                    results.append(wrap_value_with_context(country_code_dict[token], prev, nex))

    # for token in tokens_url:
    #     for i in range(0, len(token)):
    #         for j in range(i):
    #             # Cities
    #             value = token[j:i]
    #             # city = self.tries[_CITY].get(value)
    #             # if city is not None and len(value) > 4 and value not in self.tries[_STOP_WORDS]:
    #             #     dict_out[_CITY].append(value)
    #             # # States
    #             # state = self.tries[_STATE].get(value)
    #             # if state is not None and len(value) > 4 and value not in self.tries[_STOP_WORDS]:
    #             #     dict_out[_STATE].append(value)
    #             #
    #             # Countries
    #             country = self.tries[_COUNTRY].get(value)
    #             # country = None
    #             if country is not None and len(value) > 4 and value not in self.tries[_STOP_WORDS]:
    #                 country_list.append(value)

    return results if len(results) > 0 else None