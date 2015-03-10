import itertools
import json
import os
from StringIO import StringIO
from django.test import SimpleTestCase
from corehq.apps.app_manager.tests import TestFileMixin
from couchexport.export import export_raw, export_from_tables
from couchexport.models import Format

HEADERS = (('people', ('name', 'gender')), ('offices', ('location', 'name')), ('tricks', (u'unicod\u00E9', 'comm,as<')))
DATA = (
    ('people', [('danny', 'male'), ('amelia', 'female'), ('carter', 'various')]),
    ('offices', [('Delhi, India', 'DSI'), ('Boston, USA', 'Dimagi, Inc'), ('Capetown, South Africa', 'DSA')]),
    ('tricks', [(u'more \u0935', u'<p> , commas , </p>')])
)
EXPECTED = {"tricks": {"headers": [u'unicod\u00E9', 'comm,as<'], "rows": [[u'more \u0935', u'<p> , commas , </p>']]}, "offices": {"headers": ["location", "name"], "rows": [["Delhi, India", "DSI"], ["Boston, USA", "Dimagi, Inc"], ["Capetown, South Africa", "DSA"]]}, "people": {"headers": ["name", "gender"], "rows": [["danny", "male"], ["amelia", "female"], ["carter", "various"]]}}


class Tester(object):

    def __init__(self, test_case, format):
        self.test_case = test_case
        self.format = format

    def __enter__(self):
        self.buffer = StringIO()
        return self.buffer

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            if self.format == Format.JSON:
                self.test_case.assertDictEqual(json.loads(self.buffer.getvalue()), EXPECTED)

            if self.format == Format.HTML:
                path = os.path.join(os.path.dirname(__file__), "expected.html")
                with open(path, 'r') as f:
                    expected = f.read()
                self.test_case.assertHtmlEqual(expected,self.buffer.getvalue())

            if self.format == Format.UNZIPPED_CSV:
                path = os.path.join(os.path.dirname(__file__), "expected.csv")
                with open(path, 'r') as f:
                    expected = f.read()
                self.test_case.assertEqual(expected, self.buffer.getvalue())

        self.buffer.close()


class ExportRawTest(SimpleTestCase, TestFileMixin):

    def test_export_raw(self):

        with Tester(self, Format.JSON) as buffer:
            export_raw(HEADERS, DATA, buffer, format=Format.JSON)

        with Tester(self, Format.JSON) as buffer:
            # test lists
            export_raw(list(HEADERS), list(DATA), buffer, format=Format.JSON)

        with Tester(self, Format.JSON) as buffer:
            # test generators
            export_raw((h for h in HEADERS), ((name, (r for r in rows)) for name, rows in DATA), buffer, format=Format.JSON)
            
        with Tester(self, Format.JSON) as buffer:
            # test export_from_tables
            headers = dict(HEADERS)
            data = dict(DATA)
            tables = {}
            for key in set(headers.keys()) | set(data.keys()):
                tables[key] = itertools.chain([headers[key]], data[key])

            export_from_tables(tables.items(), buffer, format=Format.JSON)


class OnDiskExportTest(SimpleTestCase, TestFileMixin):

    def test_html_export(self):
        with Tester(self, Format.HTML) as buffer:
            export_raw(HEADERS, DATA, buffer, format=Format.HTML)

    def test_unzipped_csv_export(self):
        with Tester(self, Format.UNZIPPED_CSV) as buffer:
            # Only exports one sheet
            export_raw(HEADERS, DATA, buffer, format=Format.UNZIPPED_CSV)
