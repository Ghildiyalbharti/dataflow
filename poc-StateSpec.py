import argparse
import json
import logging
import pickle
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.coders import BytesCoder
from apache_beam.transforms.userstate import (
    BagStateSpec,
    TimerSpec,
    on_timer,
    TimeDomain,
)


class ParseMessage(beam.DoFn):
    def process(self, element):
        try:
            logging.info(f"Received message: {element}")
            message = json.loads(element.decode("utf-8"))
            device_id = message.get("deviceId", "unknown_device")
            yield (
                device_id,
                {
                    "deviceId": device_id,
                    "timestamp": message.get("timestamp"),
                    "soc": float(message.get("soc", 0)),
                    "temp": float(message.get("temp", 0)),
                },
            )
        except (json.JSONDecodeError, ValueError) as e:
            logging.error(f"Failed to parse message: {element}, error: {e}")
            return


class StatefulKPIProcessor(beam.DoFn):
    BUFFER = BagStateSpec("buffer", BytesCoder())
    LAST_RESULT_BUFFER = BagStateSpec("last_result", BytesCoder())
    #TIMER = TimerSpec("flush_timer", TimeDomain.WATERMARK)
    TIMER = TimerSpec("flush_timer", TimeDomain.REAL_TIME)

    

    def process(
        self,
        element,
        timestamp=beam.DoFn.TimestampParam,
        buffer_state=beam.DoFn.StateParam(BUFFER),
        last_result_state=beam.DoFn.StateParam(LAST_RESULT_BUFFER),
        flush_timer=beam.DoFn.TimerParam(TIMER),
    ):
        key, value = element
        buffer_state.add(pickle.dumps(value))
        #flush_timer.set(timestamp + beam.utils.timestamp.Duration(seconds=600))
        flush_timer.set(beam.utils.timestamp.Timestamp.now() + beam.utils.timestamp.Duration(seconds=600))

        logging.info(f"Set timer for {key} at {timestamp + beam.utils.timestamp.Duration(seconds=600)}")
        return []

    @on_timer(TIMER)
    def flush(
        self,
        buffer_state=beam.DoFn.StateParam(BUFFER),
        last_result_state=beam.DoFn.StateParam(LAST_RESULT_BUFFER),
    ):
        items = [pickle.loads(x) for x in buffer_state.read()]
        logging.info(f"Timer fired for buffer: {len(items)} items")
        buffer_state.clear()

        last_result = None
        for item in last_result_state.read():
            last_result = json.loads(item)
        last_result_state.clear()

        if not items and last_result:
            yield last_result
            return

        total_soc = 0.0
        total_temp = 0.0
        count = 0

        for v in items:
            soc = v.get("soc")
            temp = v.get("temp")
            if soc is not None and temp is not None:
                count += 1
                total_soc += soc
                total_temp += temp

        if count == 0:
            if last_result:
                yield last_result
        else:
            avg_result = {
                "window": datetime.utcnow().strftime("%Y%m%d%H%M"),
                "records_count": count,
                "avg_soc": total_soc / count,
                "avg_temp": total_temp / count,
            }
            last_result_state.add(json.dumps(avg_result).encode())
            yield avg_result


class WriteJSONToGCS(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, element):
        from apache_beam.io.gcp.gcsio import GcsIO

        window_label = element.get("window", "unknown")
        file_path = f"{self.output_path}/{window_label}.json"

        try:
            gcs = GcsIO()
            with gcs.open(file_path, "w") as f:
                f.write(json.dumps(element).encode("utf-8"))
            logging.info(f" File written to GCS: {file_path}")
        except Exception as e:
            logging.error(f" Failed to write to {file_path}: {e}")

        return []


def run(input_topic, output_path, window_size, pipeline_args=None):
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read PubSub" >> beam.io.ReadFromPubSub(topic=input_topic,timestamp_attribute="timestamp" )
            | "Parse JSON" >> beam.ParDo(ParseMessage())
            | "Window into Fixed" >> beam.WindowInto(FixedWindows(window_size * 60),
                    allowed_lateness=beam.utils.timestamp.Duration(seconds=600)
                    )
            | "Stateful KPI Buffering" >> beam.ParDo(StatefulKPIProcessor())
            | "Write to GCS" >> beam.ParDo(WriteJSONToGCS(output_path))
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
