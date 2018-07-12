import unittest
import json
from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor


class TestMetadataExtractor(unittest.TestCase):
    # value of title extraction is a string
    def test_title_extractor(self) -> None:
        hme = HTMLMetadataExtractor()
        with open('etk/unit_tests/ground_truth/sample_html.jl', 'r') as f:
            sample_html = json.load(f)
        test_html = sample_html["raw_content"]

        title_text = hme.extract(test_html, extract_title = True)[0].value
        expected_title = "323-452-2013 ESCORT ALERT! - Luna The Hot Playmate (323) 452-2013 - 23"
        self.assertEqual(title_text, expected_title)

    # value of mate extraction is a dict
    def test_meta_tag_extractor(self) -> None:
        hme = HTMLMetadataExtractor()
        with open('etk/unit_tests/ground_truth/sample_html.jl', 'r') as f:
            sample_html = json.load(f)
        test_html = sample_html["raw_content"]

        meta_tag_dic = hme.extract(test_html, extract_meta = True)[0].value
        expected_dic = {
                'description': "323-452-2013 Escort listings. View all of her listings at once. On this page you will see a history for 323-452-2013 and all of the escort ads placed with this phone number. Specifically in losangeles: 323-452-2013 ESCORT ALERT! - Luna The Hot Playmate (323) 452-2013 - 23",
                'netinsert': "0.0.1.13.13.1",
                'keywords': "323-452-2013, escort listing, escort ad, escorts in Los angeles, 323-452-2013 in Los angeles, escorts in losangeles, call 323-452-2013, phone number 323-452-2013, craigslist escorts, backpage escorts, listings"
            }

        self.assertDictEqual(meta_tag_dic, expected_dic)

    # value of microdata extraction is a list
    def test_microdata_extractor(self) -> None:
        hme = HTMLMetadataExtractor()
        test_html = """<!DOCTYPE HTML>
                    ... <html>
                    ...  <head>
                    ...   <title>Photo gallery</title>
                    ...  </head>
                    ...  <body>
                    ...   <h1>My photos</h1>
                    ...   <figure itemscope itemtype="http://n.whatwg.org/work" itemref="licenses">
                    ...    <img itemprop="work" src="images/house.jpeg" alt="A white house, boarded up, sits in a forest.">
                    ...    <figcaption itemprop="title">The house I found.</figcaption>
                    ...   </figure>
                    ...   <figure itemscope itemtype="http://n.whatwg.org/work" itemref="licenses">
                    ...    <img itemprop="work" src="images/mailbox.jpeg" alt="Outside the house is a mailbox. It has a leaflet inside.">
                    ...    <figcaption itemprop="title">The mailbox.</figcaption>
                    ...   </figure>
                    ...   <footer>
                    ...    <p id="licenses">All images licensed under the <a itemprop="license"
                    ...    href="http://www.opensource.org/licenses/mit-license.php">MIT
                    ...    license</a>.</p>
                    ...   </footer>
                    ...  </body>
                    ... </html>"""

        microdata = hme.extract(test_html, extract_microdata = True)[0].value
        expected_microdata = [{'properties': {'license': 'http://www.opensource.org/licenses/mit-license.php',
                                             'title': 'The house I found.',
                                             'work': 'http://www.example.com/images/house.jpeg'},
                                            'type': 'http://n.whatwg.org/work'},
                                            {'properties': {'license': 'http://www.opensource.org/licenses/mit-license.php',
                                             'title': 'The mailbox.',
                                             'work': 'http://www.example.com/images/mailbox.jpeg'},
                                            'type': 'http://n.whatwg.org/work'}]
        self.assertEqual(microdata, expected_microdata)

    # value of microdata extraction is list
    def test_json_ld_extractor(self) -> None:
        hme = HTMLMetadataExtractor()
        with open('etk/unit_tests/ground_truth/sample_html_for_json_ld.html', 'r') as f:
          test_html = f.read()
        
        json_ld_data = hme.extract(test_html, extract_json_ld = True)[0].value

        expected_ld_data = [{'@context': 'http://schema.org',
                          '@type': 'MusicEvent',
                          'location': {'@type': 'Place',
                                       'address': {'@type': 'PostalAddress',
                                                   'addressCountry': 'US',
                                                   'addressLocality': 'Brooklyn',
                                                   'addressRegion': 'NY',
                                                   'postalCode': '11225',
                                                   'streetAddress': '497 Rogers Ave'},
                                       'geo': {'@type': 'GeoCoordinates',
                                               'latitude': 40.660109,
                                               'longitude': -73.953193},
                                       'name': 'The Owl Music Parlor',
                                       'sameAs': 'http://www.theowl.nyc'},
                          'name': 'Elysian Fields',
                          'performer': [{'@type': 'MusicGroup',
                                         'name': 'Elysian Fields',
                                         'sameAs': 'https://www.songkick.com/artists/236156-elysian-fields?utm_medium=organic&utm_source=microformat'}],
                          'startDate': '2017-06-10T19:30:00-0400',
                          'url': 'https://www.songkick.com/concerts/30173984-elysian-fields-at-owl-music-parlor?utm_medium=organic&utm_source=microformat'},
                         {'@context': 'http://schema.org',
                          '@type': 'MusicEvent',
                          'location': {'@type': 'Place',
                                       'address': {'@type': 'PostalAddress',
                                                   'addressCountry': 'US',
                                                   'addressLocality': 'San Francisco',
                                                   'addressRegion': 'CA',
                                                   'postalCode': '94107',
                                                   'streetAddress': '500 Fourth Street'},
                                       'geo': {'@type': 'GeoCoordinates',
                                               'latitude': 37.7795638,
                                               'longitude': -122.398023},
                                       'name': 'Hotel Utah Saloon',
                                       'sameAs': 'http://www.hotelutah.com/'},
                          'name': 'Elysian Fields',
                          'performer': [{'@type': 'MusicGroup',
                                         'name': 'Elysian Fields',
                                         'sameAs': 'https://www.songkick.com/artists/236156-elysian-fields?utm_medium=organic&utm_source=microformat'},
                                        {'@type': 'MusicGroup',
                                         'name': 'Chocolate Genius Inc.',
                                         'sameAs': 'https://www.songkick.com/artists/1009602-chocolate-genius-inc?utm_medium=organic&utm_source=microformat'}],
                          'startDate': '2017-04-26T20:00:00-0700',
                          'url': 'https://www.songkick.com/concerts/29673614-elysian-fields-at-hotel-utah-saloon?utm_medium=organic&utm_source=microformat'},
                         {'@context': 'http://schema.org',
                          '@type': 'MusicEvent',
                          'location': {'@type': 'Place',
                                       'address': {'@type': 'PostalAddress',
                                                   'addressCountry': 'France',
                                                   'addressLocality': 'Saint-Nazaire',
                                                   'postalCode': '44600',
                                                   'streetAddress': 'Alvéole 14 de la base '
                                                                    'sous-Marine Bd de la '
                                                                    'Légion d’Honneur'},
                                       'geo': {'@type': 'GeoCoordinates',
                                               'latitude': 47.2755434,
                                               'longitude': -2.2022817},
                                       'name': 'VIP',
                                       'sameAs': 'http://www.levip-saintnazaire.com/'},
                          'name': 'Elysian Fields',
                          'performer': [{'@type': 'MusicGroup',
                                         'name': 'Elysian Fields',
                                         'sameAs': 'https://www.songkick.com/artists/236156-elysian-fields?utm_medium=organic&utm_source=microformat'},
                                        {'@type': 'MusicGroup',
                                         'name': 'Troy Von Balthazar',
                                         'sameAs': 'https://www.songkick.com/artists/355304-troy-von-balthazar?utm_medium=organic&utm_source=microformat'}],
                          'startDate': '2016-10-29T21:00:00+0200',
                          'url': 'https://www.songkick.com/concerts/27626524-elysian-fields-at-vip?utm_medium=organic&utm_source=microformat'},
                         {'@context': 'http://schema.org',
                          '@type': 'MusicGroup',
                          'image': 'https://images.sk-static.com/images/media/profile_images/artists/236156/card_avatar',
                          'interactionCount': '6066 UserLikes',
                          'logo': 'https://images.sk-static.com/images/media/profile_images/artists/236156/card_avatar',
                          'name': 'Elysian Fields',
                          'url': 'https://www.songkick.com/artists/236156-elysian-fields?utm_medium=organic&utm_source=microformat'}]
        self.assertEqual(json_ld_data, expected_ld_data)

    # value of microdata extraction is a list
    def test_rdfa_extrator(self) -> None:
        hme = HTMLMetadataExtractor()
        test_html = """<html>
                    ...  <head>
                    ...    ...
                    ...  </head>
                    ...  <body prefix="dc: http://purl.org/dc/terms/ schema: http://schema.org/">
                    ...    <div resource="/alice/posts/trouble_with_bob" typeof="schema:BlogPosting">
                    ...       <h2 property="dc:title">The trouble with Bob</h2>
                    ...       <h3 property="dc:creator schema:creator" resource="#me">Alice</h3>
                    ...       <div property="schema:articleBody">
                    ...         <p>The trouble with Bob is that he takes much better photos than I do:</p>
                    ...       </div>
                    ...    </div>
                    ...  </body>
                    ... </html>
                    ... """

        rdfa_data = hme.extract(test_html, extract_rdfa = True)[0].value
        expected_rdfa_data = [{'@id': 'http://www.example.com/alice/posts/trouble_with_bob',
                                        '@type': ['http://schema.org/BlogPosting'], 
                                        'http://purl.org/dc/terms/creator': [{'@id': 'http://www.example.com/#me'}], 
                                        'http://purl.org/dc/terms/title': [{'@value': 'The trouble with Bob'}], 
                                        'http://schema.org/articleBody': [{'@value': '\n                    ...         The trouble with Bob is that he takes much better photos than I do:\n                    ...       '}],
                                        'http://schema.org/creator': [{'@id': 'http://www.example.com/#me'}]}]
        self.assertEqual(rdfa_data, expected_rdfa_data)


if __name__ == '__main__':
    unittest.main()