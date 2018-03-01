import unittest
from etk.etk import ETK

sample_input = {
    "projects": [
        {
            "name": "etk",
            "description": "version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep and others."
        },
        {
            "name": "rltk",
            "description": "record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students."
        }
    ]
}


class TestSegment(unittest.TestCase):

    def test_segment(self) -> None:
        etk = ETK()
        doc = etk.create_document(sample_input)
        descriptions = doc.select_segments("projects[*].description")
        description_value = [i.value for i in descriptions]
        expected = [
            'version 2 of etk, implemented by Runqi, Dongyu, Sylvia, Amandeep and others.',
            'record linkage toolkit, implemented by Pedro, Mayank, Yixiang and several students.'
        ]
        self.assertEqual(description_value, expected)


if __name__ == '__main__':
    unittest.main()