import argparse
import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


class ParseMessage(beam.DoFn):
    """
    Parse message string (JSON) and emit a dict with needed values.
    Expected input payload: {"deviceId": "...", "timestamp": "...", "soc": 32, "temp": 14500}
    """

    def process(self, element):
        message = json.loads(element.decode("utf-8"))
        yield {
            "deviceId": message.get("deviceId"),
            "timestamp": message.get("timestamp"),
            "soc": float(message.get("soc", 0)),
            "temp": float(message.get("temp", 0)),
        }


class AddWindowKey(beam.DoFn):
    """
    Emit a fixed key so all elements in a window get grouped together.
    """

    def process(self, element, window=beam.DoFn.WindowParam):
        window_label = f"{window.start.to_utc_datetime().strftime('%Y%m%d%H%M')}" \
                       f"-{window.end.to_utc_datetime().strftime('%Y%m%d%H%M')}"
        yield window_label, element


class ComputeAvg(beam.DoFn):
    """
    For each window, compute average SOC and temp.
    """

    def process(self, window_key_values):
        window_key, values = window_key_values

        count = 0
        total_soc = 0.0
        total_temp = 0.0

        for v in values:
            count += 1
            total_soc += v['soc']
            total_temp += v['temp']

        avg_soc = total_soc / count if count > 0 else 0
        avg_temp = total_temp / count if count > 0 else 0

        result = {
            "window": window_key,
            "records_count": count,
            "avg_soc": avg_soc,
            "avg_temp": avg_temp,
        }
        yield window_key, result


class WriteJSONToGCS(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, kv):
        window_key, result_dict = kv
        file_path = f"{self.output_path}/{window_key}.json"

        with beam.io.gcsio.GcsIO().open(file_path, "w") as f:
            f.write(json.dumps(result_dict).encode("utf-8"))


def run(input_topic, output_path, window_size, pipeline_args=None):
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read PubSub" >> beam.io.ReadFromPubSub(topic=input_topic)
            | "Parse JSON" >> beam.ParDo(ParseMessage())
            | "Window" >> beam.WindowInto(FixedWindows(int(window_size) * 60))
            | "Add Window Key" >> beam.ParDo(AddWindowKey())
            | "Group By Window" >> beam.GroupByKey()
            | "Compute Average" >> beam.ParDo(ComputeAvg())
            | "Write JSON" >> beam.ParDo(WriteJSONToGCS(output_path))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_topic", required=True)
    parser.add_argument("--window_size", default=10, type=int, help="In minutes")
    parser.add_argument("--output_path", required=True, help="gs://bucket/folder")
    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_topic,
        known_args.output_path,
        known_args.window_size,
        pipeline_args,
    )
