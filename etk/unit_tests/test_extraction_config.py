import unittest
import sys
sys.path.append('../../')
from etk.core import Core
from etk import core


class TestExtractionConfig(unittest.TestCase):

    def setUp(self):
        self.c = Core()

    def test_extraction_policy(self):
        config = {}
        self.assertEqual(self.c.determine_extraction_policy(config), core._REPLACE)
        config = {'extraction_policy': 'replace'}
        self.assertEqual(self.c.determine_extraction_policy(config), core._REPLACE)
        config = {'extraction_policy': 'keep_existing'}
        self.assertEqual(self.c.determine_extraction_policy(config), core._KEEP_EXISTING)
        config = None
        self.assertEqual(self.c.determine_extraction_policy(config), core._REPLACE)
        config = {'extraction_policy': 'something'}
        with self.assertRaises(ValueError):
            self.c.determine_extraction_policy(config)

    def test_determine_segment(self):
        full_path = ''
        self.assertEqual(self.c.determine_segment(full_path), core._SEGMENT_OTHER)
        full_path = 'content_extraction.title'
        self.assertEqual(self.c.determine_segment(full_path), core._SEGMENT_TITLE)
        full_path = 'content_extraction.inferlink_extractions.inferlink_description'
        self.assertEqual(self.c.determine_segment(full_path), core._SEGMENT_INFERLINK_DESC)
        full_path = 'content_extraction.content_relaxed'
        self.assertEqual(self.c.determine_segment(full_path), core._SEGMENT_READABILITY_RELAXED)


if __name__ == '__main__':
    unittest.main()