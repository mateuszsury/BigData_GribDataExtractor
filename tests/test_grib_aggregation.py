import math
from pathlib import Path
import sys
from types import SimpleNamespace
from datetime import datetime
import tempfile
import unittest
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'GRIBExtractor_GUI_Win'))
from grib_methods import Grib_file, aggregate_monthly, load_dictionary

class MonthlyTests(unittest.TestCase):
    def test_real_unordered_partial_year_grib_matches_reference(self):
        result = Grib_file(str(Path(__file__).parent / 'fixtures/synthetic.grib2'))
        self.assertEqual(2024, result.rok)
        self.assertEqual(['2 metre temperature', 'Surface pressure'], result.variables_to_grab1)
        self.assertEqual([285.0, 293.0], result.wyniki['2 metre temperature'][:2])
        self.assertEqual([100003.0, 100103.0], result.wyniki['Surface pressure'][:2])
        self.assertTrue(all(math.isnan(x) for x in result.wyniki['2 metre temperature'][2:]))

    def test_masked_and_nonfinite_cells_do_not_bias_monthly_mean(self):
        message = SimpleNamespace(validDate=datetime(2024, 2, 29), name='sample',
            values=np.ma.array([2.0, 4.0, 9999.0, np.nan], mask=[0, 0, 1, 0]))
        year, names, values = aggregate_monthly([message])
        self.assertEqual(3.0, values['sample'][1])
        with self.assertRaisesRegex(ValueError, 'one calendar year'):
            aggregate_monthly([message, SimpleNamespace(validDate=datetime(2025, 1, 1), name='sample')])

    def test_empty_file_is_rejected_and_empty_folder_has_a_defined_result(self):
        with self.assertRaisesRegex(ValueError, 'no messages'):
            aggregate_monthly([])
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(([], [], {}), load_dictionary(folder))

if __name__ == '__main__':
    unittest.main()
