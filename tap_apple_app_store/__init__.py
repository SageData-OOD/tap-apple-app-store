#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform
from datetime import datetime, timedelta
from app_store_scraper import AppStore

REQUIRED_CONFIG_KEYS = ["country_code", "list_app_name"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def get_key_properties(stream_name):
    return ["app_id", "date", "userName"]


def create_metadata_for_report(schema, tap_stream_id):
    key_properties = get_key_properties(tap_stream_id)

    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available", "forced-replication-method": "FULL_TABLE",
                                             "table-key-properties": key_properties}}]

    for key in schema.properties:
        inclusion = "automatic" if key in key_properties else "available"
        mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(schema, stream_id)
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


def date_to_str(o):
    if isinstance(o, datetime):
        return o.__str__()


def sync_stream(config, state, stream):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )

    # Having start_date will slow down the process
    start_date = None
    if config.get("start_date"):
        start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")

    for app_name in config["list_app_name"]:
        app = AppStore(country=config["country_code"], app_name=app_name)

        how_many = config.get("how_many")
        if how_many:
            app.review(how_many=how_many, after=start_date)
        else:
            app.review(after=start_date)

        records = json.loads(json.dumps(app.reviews, default=date_to_str))

        with singer.metrics.record_counter(stream.tap_stream_id) as counter:
            for row in records:
                # Type Conversation and Transformation
                row["country"] = app.country
                row["app_id"] = app.app_id
                row["app_name"] = app.app_name

                transformed_data = transform(row, schema, metadata=mdata)

                # write one or more rows to the stream:
                singer.write_records(stream.tap_stream_id, [transformed_data])
                counter.increment()


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        sync_stream(config, state, stream)
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        state = args.state or {}
        sync(args.config, state, catalog)


if __name__ == "__main__":
    main()
