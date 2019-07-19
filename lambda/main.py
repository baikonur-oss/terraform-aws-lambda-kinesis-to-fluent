import base64
import datetime
import gzip
import json
import logging
import os
import time
from json import JSONDecodeError

import boto3
import dateutil.parser
from aws_kinesis_agg.deaggregator import iter_deaggregate_records
from aws_xray_sdk.core import xray_recorder, patch
from fluent import sender

# set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info('Loading function')

# when debug logging is needed, uncomment following lines:
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)

# patch boto3 with X-Ray
libraries = ('boto3', 'botocore')
patch(libraries)

# global client instances
s3 = boto3.client('s3')

# configure with env vars
FAILED_LOG_S3_BUCKET = os.environ['FAILED_LOG_S3_BUCKET']
FAILED_LOG_S3_PATH_PREFIX = os.environ['FAILED_LOG_S3_PREFIX']

LOG_TYPE_FIELD: str = os.environ['LOG_TYPE_FIELD']
LOG_TIMESTAMP_FIELD: str = os.environ['LOG_TIMESTAMP_FIELD']
LOG_TYPE_FIELD_WHITELIST: set = set(str(os.environ['LOG_TYPE_WHITELIST']).split(','))
LOG_TYPE_UNKNOWN_PREFIX: str = os.environ['LOG_TYPE_UNKNOWN_PREFIX']

LOG_FLUENT_TARGET_URL = os.environ['LOG_FLUENT_TARGET_URL']
LOG_FLUENT_TAG = os.environ['LOG_FLUENT_TAG']  # second-level tag


def append_to_dict(dictionary: dict, log_type: str, log_data: object, log_timestamp=None):
    if log_type not in dictionary:
        # we've got first record for this type, initialize value for type

        # first record timestamp to use in file path
        if log_timestamp:
            try:
                log_timestamp = dateutil.parser.parse(log_timestamp)
            except TypeError:
                logger.error(f"Bad timestamp: {log_timestamp}")
                logger.info(f"Falling back to current time for type \"{log_type}\"")
                log_timestamp = datetime.datetime.now()
        else:
            logger.info(f"No timestamp for first record")
            logger.info(f"Falling back to current time for type \"{log_type}\"")
            log_timestamp = datetime.datetime.now()

        dictionary[log_type] = {
            'records':         list(),
            'first_timestamp': log_timestamp,
        }

    dictionary[log_type]['records'].append(log_data)


def normalize_kinesis_payload(payload: dict):
    # Normalize messages from CloudWatch (subscription filters) and pass through anything else
    # https://docs.aws.amazon.com/ja_jp/AmazonCloudWatch/latest/logs/SubscriptionFilters.html

    logger.debug(f"normalizer input: {payload}")

    payloads = []

    if len(payload) < 1:
        logger.error(f"Got weird record: \"{payload}\", skipping")
        return payloads

    # check if data is JSON and parse
    try:
        payload = json.loads(payload)

    except JSONDecodeError:
        logger.error(f"Non-JSON data found: {payload}, skipping")
        return payloads

    if 'messageType' in payload:
        logger.debug(f"Got payload looking like CloudWatch Logs via subscription filters: "
                     f"{payload}")

        if payload['messageType'] == "DATA_MESSAGE":
            if 'logEvents' in payload:
                for event in payload['logEvents']:
                    # check if data is JSON and parse
                    try:
                        logger.debug(f"message: {event['message']}")
                        payload_parsed = json.loads(event['message'])
                        logger.debug(f"parsed payload: {payload_parsed}")

                    except JSONDecodeError:
                        logger.debug(f"Non-JSON data found inside CWL message: {event}, giving up")
                        continue

                    payloads.append(payload_parsed)

            else:
                logger.error(f"Got DATA_MESSAGE from CloudWatch but logEvents are not present, "
                             f"skipping payload: {payload}")

        elif payload['messageType'] == "CONTROL_MESSAGE":
            logger.info(f"Got CONTROL_MESSAGE from CloudWatch: {payload}, skipping")
            logger.info(f"Skipping CONTROL_MESSAGE")
            return payloads

        else:
            logger.error(f"Got unknown messageType, shutting down")
            raise ValueError(f"Unknown messageType: {payload}")
    else:
        payloads.append(payload)
        logger.debug(f"After append: {payloads}")
    return payloads


def connect_fluentd(hostname, tag):
    try:
        client = sender.FluentSender(tag, hostname)
        logger.info(f"Connected to fluentd at {hostname}")
        return client
    except Exception:
        logger.error(f"Failed to connect to fluentd at {hostname}")
        raise


def send_fluent_raw(fluentd, tag: str, timestamp, data: bytes):
    try:
        message = {"message": data}
        logger.debug(f"Raw fluentd message: {message}")

        if fluentd.emit_with_time(tag, timestamp, message):
            return True
        else:
            logger.error(fluentd.last_error)
            fluentd.clear_last_error()
            return False

    except Exception as e:
        logger.error(f"[send_fluent_json] Failed to send bytes: {data}")
        logger.error(e)
        import traceback
        traceback.print_stack()
        return False


def apply_whitelist(log_dict: dict, whitelist: set):
    retval = dict()
    logger.debug(whitelist)
    logger.debug(log_dict)
    if len(whitelist) == 0 or whitelist == ['']:
        logger.debug(log_dict)
        return log_dict

    for entry in whitelist:
        if entry in log_dict:
            retval[entry] = log_dict[entry]
    logger.debug(retval)
    return retval


def send_by_type(log_dict: dict):
    processed_records = 0
    subsegment = xray_recorder.begin_subsegment('send all records')
    for log_type in log_dict:
        xray_recorder.begin_subsegment(f"send {log_type} records")
        record_list = log_dict[log_type]['records']
        logger.debug(record_list)

        logger.info(f"Connecting to fluentd endpoint {LOG_FLUENT_TARGET_URL} with top-level tag {log_type}")
        fluentd = connect_fluentd(LOG_FLUENT_TARGET_URL, log_type)
        # on connection error an exception is raised by fluent-sender
        # and lambda is terminated with "failed" status (as expected)

        logger.info(f"Sending {len(record_list)} {log_type} records to fluentd endpoint")
        for record in record_list:
            logger.debug(record)

            try:
                t = dateutil.parser.parse(record[LOG_TIMESTAMP_FIELD])
                timestamp = int(time.mktime(t.timetuple()))
            except (KeyError, ValueError):
                logger.info("[send_fluent_json] Timestamp not found, using fallback_time")
                timestamp = time.time()

            raw_data: bytes = json.dumps(record).encode()

            if not send_fluent_raw(fluentd, LOG_FLUENT_TAG, timestamp, raw_data):
                # something bad happened and we failed to send data to fluentd endpoint
                logger.error(f"Failed to send data: {record}")

                # raise an exception to terminates lambda with "failed" status
                raise NameError("error in send_fluent_raw")

        fluentd.close()
        logger.info(f"Successfully sent {len(record_list)} {log_type} records")
        processed_records += len(record_list)
        xray_recorder.end_subsegment()

    subsegment.put_annotation("processed_records", processed_records)
    xray_recorder.end_subsegment()


def decode_validate(raw_records: list):
    xray_recorder.begin_subsegment('decode and validate')

    log_dict = dict()

    processed_records = 0

    for record in iter_deaggregate_records(raw_records):
        logger.debug(f"raw Kinesis record: {record}")
        # Kinesis data is base64 encoded
        decoded_data = base64.b64decode(record['kinesis']['data'])

        # check if base64 contents is gzip
        # gzip magic number 0x1f 0x8b
        if decoded_data[0] == 0x1f and decoded_data[1] == 0x8b:
            decoded_data = gzip.decompress(decoded_data)

        decoded_data = decoded_data.decode()
        normalized_payloads = normalize_kinesis_payload(decoded_data)
        logger.debug(f"Normalized payloads: {normalized_payloads}")

        for normalized_payload in normalized_payloads:
            logger.debug(f"Parsing normalized payload: {normalized_payload}")

            processed_records += 1

            # get log id when available
            # log_id = normalized_payload.setdefault(LOG_ID_FIELD, None)
            # Notice: log_id is not used in this Lambda

            # check if log type field is available
            try:
                log_type = normalized_payload[LOG_TYPE_FIELD]
            except KeyError:
                logger.error(f"Cannot retrieve necessary field \"{LOG_TYPE_FIELD}\" from payload, "
                             f"skipping: {normalized_payload}")
                continue

            # check if timestamp is present
            try:
                timestamp = normalized_payload[LOG_TIMESTAMP_FIELD]

            except KeyError:
                logger.error(f"Cannot retrieve necessary field \"{LOG_TIMESTAMP_FIELD}\" from payload, "
                             f"skipping: {normalized_payload}")
                log_type += "_no_timestamp"
                logger.error(f"Re-marking as {log_type} and giving up")
                append_to_dict(log_dict, log_type, normalized_payload)
                continue

            # valid data
            append_to_dict(log_dict, log_type, normalized_payload, log_timestamp=timestamp)

    logger.info(f"Processed {processed_records} records from Kinesis")
    xray_recorder.end_subsegment()
    return log_dict


def handler(event, context):
    logger.info("Started")
    logger.debug(f"event: {event}")
    logger.debug(f"context: {context}")

    raw_records = event['Records']

    log_dict = decode_validate(raw_records)
    log_dict_filtered: dict = apply_whitelist(log_dict, LOG_TYPE_FIELD_WHITELIST)
    send_by_type(log_dict_filtered)

    logger.info(f"Finished successfully")
