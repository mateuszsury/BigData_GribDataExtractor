"""Generate synthetic GRIB2 values; no external meteorological data are used."""
from pathlib import Path
import eccodes

records = [
    ('2t', 20240201, [290, 292, 294, 296]),
    ('sp', 20240101, [100000, 100002, 100004, 100006]),
    ('2t', 20240101, [280, 282, 284, 286]),
    ('sp', 20240201, [100100, 100102, 100104, 100106]),
    ('2t', 20240102, [284, 286, 288, 290]),
]

if __name__ == '__main__':
    target = Path(__file__).parent / 'fixtures/synthetic.grib2'
    with target.open('wb') as output:
        for variable, date, values in records:
            message = eccodes.codes_grib_new_from_samples('regular_ll_sfc_grib2')
            try:
                for key, value in {'Ni': 2, 'Nj': 2, 'latitudeOfFirstGridPointInDegrees': 1,
                    'longitudeOfFirstGridPointInDegrees': 0, 'latitudeOfLastGridPointInDegrees': 0,
                    'longitudeOfLastGridPointInDegrees': 1, 'iDirectionIncrementInDegrees': 1,
                    'jDirectionIncrementInDegrees': 1, 'dataDate': date, 'dataTime': 0,
                    'shortName': variable}.items():
                    eccodes.codes_set(message, key, value)
                eccodes.codes_set_values(message, values)
                eccodes.codes_write(message, output)
            finally:
                eccodes.codes_release(message)
