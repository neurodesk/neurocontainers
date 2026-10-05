"""Runtime checks for scanner time-series transport using real MRD images."""

import unittest

import ismrmrd
import numpy as np

import frisgo


class RecordingConnection:
    def __init__(self, images):
        self.images = images
        self.outputs = []
        self.logs = []
        self.closed = False

    def __iter__(self):
        return iter(self.images + [None])

    def send_image(self, images):
        self.outputs.extend(images)

    def send_logging(self, level, message):
        self.logs.append(message)

    def send_close(self):
        self.closed = True


def image(repetition=0, slice_index=0, **counters):
    result = ismrmrd.Image.from_array(
        np.full((2, 3), 100 + repetition, np.uint16), transpose=False
    )
    result.image_series_index = 1
    result.image_type = ismrmrd.IMTYPE_MAGNITUDE
    result.repetition = repetition
    result.slice = slice_index
    result.position[:] = (0, 0, slice_index)
    result.field_of_view[:] = (3, 2, 1)
    result.read_dir[:] = (1, 0, 0)
    result.phase_dir[:] = (0, 1, 0)
    result.slice_dir[:] = (0, 0, 1)
    for name, value in counters.items():
        setattr(result, name, value)
    meta = ismrmrd.Meta()
    meta["SeriesDescription"] = "BOLD"
    meta["SeriesInstanceUID"] = "1.2.3"
    meta["RepetitionTime"] = "1000"
    result.attribute_string = meta.serialize()
    return result


class FrisgoOpenReconTests(unittest.TestCase):
    def test_incomplete_series_preserves_originals_and_other_groups(self):
        images = [image(0, 0), image(0, 1), image(1, 0), image(0, contrast=1)]
        connection = RecordingConnection(images)
        frisgo.process(connection, "frisgo", None)
        self.assertTrue(connection.closed)
        self.assertEqual(len(connection.outputs), len(images))
        for source, output in zip(images, connection.outputs):
            np.testing.assert_array_equal(source.data, output.data)
            self.assertEqual(source.repetition, output.repetition)
            self.assertEqual(source.slice, output.slice)
        self.assertTrue(any("incomplete time series" in log for log in connection.logs))

    def test_original_only_does_not_require_a_complete_grid(self):
        images = [image(0, 0), image(0, 1), image(1, 0)]
        connection = RecordingConnection(images)
        frisgo.process(connection, {"sendfrisgo": False}, None)
        self.assertEqual(len(connection.outputs), len(images))
        self.assertEqual(connection.logs, [])

    def test_independent_scanner_counters_are_separate_time_series(self):
        for counter in ("contrast", "phase", "set", "average"):
            with self.subTest(counter=counter):
                images = [
                    image(t, **{counter: value})
                    for t in range(4) for value in range(2)
                ]
                groups = frisgo._source_series_groups(images)
                self.assertEqual(len(groups), 2)
                for group in groups:
                    series = frisgo.assemble_time_series(group)
                    self.assertEqual(series.n_repetitions, 4)
                    self.assertEqual(len({getattr(i, counter) for i in group}), 1)

    def test_missing_whole_repetition_rejects_correction(self):
        images = [image(t) for t in (0, 2, 3, 4)]
        with self.assertRaisesRegex(ValueError, "repetition.*gap"):
            frisgo.assemble_time_series(images)

    def test_duplicate_nonconstant_repetition_rejects_correction(self):
        images = [image(t) for t in (0, 1, 1, 2)]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            frisgo.assemble_time_series(images)

    def test_constant_counter_uses_arrival_order_per_slice(self):
        images = [image(7, z) for t in range(4) for z in (1, 0)]
        series = frisgo.assemble_time_series(images)
        self.assertEqual((series.n_repetitions, series.n_slices), (4, 2))
        self.assertEqual([i.slice for i in series.grid[0]], [0, 1])

    def test_long_original_series_splits_before_mrd_index_overflow(self):
        class BoundaryConnection(RecordingConnection):
            count = 0

            def send_image(self, images):
                for output in images:
                    if self.count >= 65534:
                        self.outputs.append(output)
                    self.count += 1

        connection = BoundaryConnection([image()] * 65536)
        frisgo.process(connection, {"sendfrisgo": False}, None)
        self.assertEqual(connection.count, 65536)
        boundary = connection.outputs
        self.assertEqual(
            [(i.image_series_index, i.image_index) for i in boundary],
            [(100, 65535), (102, 1)],
        )
        metas = [ismrmrd.Meta.deserialize(i.attribute_string) for i in boundary]
        self.assertNotEqual(metas[0]["SeriesInstanceUID"], metas[1]["SeriesInstanceUID"])
        self.assertEqual([m["NumberInSeries"] for m in metas], ["65535", "1"])
        frisgo._validate_outputs(boundary)

    def test_output_validation_rejects_zero_image_index(self):
        with self.assertRaisesRegex(ValueError, "image_index"):
            frisgo._validate_outputs([image()])

    def test_scanner_minihead_keeps_geometry_and_gets_new_storage_identity(self):
        source = image(5, 2, contrast=1)
        meta = ismrmrd.Meta.deserialize(source.attribute_string)
        meta["ImageTypeValue3"] = "M"
        meta["IceMiniHead"] = frisgo.i2i._encode_ice_minihead(
            '<ParamMap."DICOM">\n{\n'
            '  <ParamString."SeriesInstanceUID"> { "1.2.3" }\n'
            '  <ParamString."SOPInstanceUID"> { "1.2.3.4" }\n'
            '  <ParamString."ImageTypeValue3"> { "M" }\n'
            '  <ParamLong."NumberInSeries"> { 99 }\n'
            '}\n<ParamMap."CONTROL">\n{\n'
            '  <ParamLong."ChronSliceNo"> { 2 }\n'
            '}\n'
        )
        source.attribute_string = meta.serialize()
        series = frisgo.TimeSeries([[source]], 1.0)
        for output in (
            next(frisgo._original_outputs([source], 100)),
            next(frisgo._frisgo_outputs(series, series.volume() + 1, 101)),
        ):
            returned_meta = ismrmrd.Meta.deserialize(output.attribute_string)
            minihead = frisgo.i2i._image_minihead(output)
            self.assertEqual((output.slice, output.repetition, output.contrast), (2, 5, 1))
            np.testing.assert_array_equal(output.position, source.position)
            np.testing.assert_array_equal(output.field_of_view, source.field_of_view)
            self.assertEqual(returned_meta["Keep_image_geometry"], "1")
            self.assertNotIn("ImageTypeValue3", returned_meta)
            self.assertNotIn("ImageTypeValue3", minihead)
            self.assertNotEqual(returned_meta["SeriesInstanceUID"], "1.2.3")
            for key in ("SeriesInstanceUID", "SOPInstanceUID"):
                self.assertEqual(
                    returned_meta[key], frisgo.i2i._minihead_string_value(minihead, key)
                )
            self.assertEqual(frisgo.i2i._minihead_long_value(minihead, "NumberInSeries"), 1)
            self.assertEqual(frisgo.i2i._minihead_long_value(minihead, "ChronSliceNo"), 2)
            frisgo._validate_outputs([output])


if __name__ == "__main__":
    unittest.main()
