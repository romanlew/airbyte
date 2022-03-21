#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import logging

from datetime import datetime
from abc import ABC
from typing import Any, Iterable, Iterator, List, Mapping, MutableMapping, Tuple

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    ConfiguredAirbyteStream,
    SyncMode,
)

from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import IncrementalMixin 
from airbyte_cdk.sources.utils.sentry import AirbyteSentry
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from airbyte_cdk.sources.utils.schema_helpers import InternalConfig

from .smart_contract import SmartContract
from .schemas.dynamic_event_schema import DynamicEventSchema


class EthereumSmartContractStreamAdapter(object):
    """Adapter class wrapping all dynamic stream events"""

    def __init__(self, namespace, smart_contract: SmartContract):
        self.__namespace = namespace
        self.__smart_contract = smart_contract

    @property
    def streams(self) -> List[Stream]:
        event_streams = list()
        all_events = self.__smart_contract.get_all_events_from_abi()

        for event_name in all_events:
            event_stream = DynamicEthereumSmartContractEventStream(namespace=self.__namespace, event_name=event_name, smart_contract=self.__smart_contract)
            event_streams.append(event_stream)
        
        return event_streams


class DynamicEthereumSmartContractEventStream(Stream, IncrementalMixin, ABC):
    """Dynamic stream class based on smart contract event"""

    transformer: TypeTransformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    # state_checkpoint_interval = None

    cursor_field = "block"

    def __init__(self, namespace: str, event_name: str, smart_contract: SmartContract, **kwargs):
        super().__init__(**kwargs)
        self.__event_name = event_name
        self.__smart_contract = smart_contract

        event_abi = self.__smart_contract.get_event_from_abi(event_name=self.__event_name)

        self.__dynamic_schema = DynamicEventSchema(namespace=namespace, event_abi=event_abi)

        self.start_block = self.__smart_contract.deployed_at_block
        self._cursor_value = None
        self.__smart_contract.logger = self.logger

    @property
    def primary_key(self) -> List[str]:
        return self.__dynamic_schema.primary_key

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        else:
            return {self.cursor_field: self.start_block}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]

    @property
    def name(self) -> str:
        return self.__dynamic_schema.name

    def get_json_schema(self):
        return self.__dynamic_schema.schema

    def as_airbyte_stream(self):
        return self.__dynamic_schema.stream

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        with AirbyteSentry.start_transaction("read_records", self.name), AirbyteSentry.start_transaction_span("read_records"):
            from_block = self.__smart_contract.deployed_at_block
            to_block = self.__smart_contract.latest_block

            if self.state and self.cursor_field in self.state:
                from_block = max(from_block, self.state[self.cursor_field])

            records = self.__smart_contract.get_events(event_name=self.__event_name, from_block=from_block, to_block=to_block)
            latest_record = max(records, key=lambda x:x['block'])
            self.state = { self.cursor_field: max(self.state[self.cursor_field], latest_record[self.cursor_field] + 1) }
            
            yield from records
            
            # if self.use_cache:
            #     # use context manager to handle and store cassette metadata
            #     with self.cache_file as cass:
            #         self.cassete = cass
            #         # vcr tries to find records based on the request, if such records exist, return from cache file
            #         # else make a request and save record in cache file
            #         response = self._send_request(request, request_kwargs)

    # def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
    #     """
    #     TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

    #     Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
    #     This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
    #     section of the docs for more information.

    #     The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
    #     necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
    #     This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

    #     An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
    #     craft that specific request.

    #     For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
    #     this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
    #     till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
    #     the date query param.
    #     """
    #     raise NotImplementedError(
    #         "Implement stream slices or delete this method!")


# Source
class SourceEthereumSmartContract(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Implements various input parameter checks:
        Connection check
        Validates entered smart contract address
        Validates entered ABI file
        """

        try:
            smart_contract_args = {
                "connection": config["blockchain"]["connection"],
                "smart_contract_address": config["smart_contract_address"],
                "smart_contract_abi": config["smart_contract_abi"],
                "smart_contract_deployed_at_block": config["start_from_block"] if "start_from_block" in config else 0,
                "logger": logger
            }
            
            smart_contract = SmartContract(**smart_contract_args)

            if not smart_contract.check_connection():
                return False, "No connection"

            if not smart_contract.check_smart_contract_address():
                return False, "Incorrect address"
            
            if not smart_contract.check_abi_file():
                return False, "Incorrect ABI file"

            return True, None
        except Exception as exc:
            return False, "An exception occurred: {exception}".format(exception=str(exc))

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        smart_contract_args = {
            "connection": config["blockchain"]["connection"],
            "smart_contract_address": config["smart_contract_address"],
            "smart_contract_abi": config["smart_contract_abi"],
            "smart_contract_deployed_at_block": config["start_from_block"] if "start_from_block" in config else 0
        }
        
        smart_contract = SmartContract(**smart_contract_args)

        args = {
            "namespace": config["namespace"],
            "smart_contract": smart_contract
        }

        smart_contract_stream = EthereumSmartContractStreamAdapter(**args)

        return smart_contract_stream.streams
    
    def _read_incremental(
        self,
        logger: logging.Logger,
        stream_instance: Stream,
        configured_stream: ConfiguredAirbyteStream,
        connector_state: MutableMapping[str, Any],
        internal_config: InternalConfig,
    ) -> Iterator[AirbyteMessage]:
        """Read stream using incremental algorithm
        TODO: This is a workaround until custom namespace support will be provided for Streams.

        :param logger:
        :param stream_instance:
        :param configured_stream:
        :param connector_state:
        :param internal_config:
        :return:
        """
        stream_name = configured_stream.stream.name

        # TODO: Q&D workaound for custom namespace support
        namespace = configured_stream.stream.namespace

        stream_state = connector_state.get(stream_name, {})
        if stream_state and "state" in dir(stream_instance):
            stream_instance.state = stream_state
            logger.info(f"Setting state of {stream_name} stream to {stream_state}")

        slices = stream_instance.stream_slices(
            cursor_field=configured_stream.cursor_field,
            sync_mode=SyncMode.incremental,
            stream_state=stream_state,
        )
        total_records_counter = 0
        for _slice in slices:
            records = stream_instance.read_records(
                sync_mode=SyncMode.incremental,
                stream_slice=_slice,
                stream_state=stream_state,
                cursor_field=configured_stream.cursor_field or None,
            )
            for record_counter, record_data in enumerate(records, start=1):
                # TODO: Q&D workaound for custom namespace support
                yield self._as_airbyte_record(stream_name, record_data, namespace)
                stream_state = stream_instance.get_updated_state(stream_state, record_data)
                checkpoint_interval = stream_instance.state_checkpoint_interval
                if checkpoint_interval and record_counter % checkpoint_interval == 0:
                    yield self._checkpoint_state(stream_instance, stream_state, connector_state)

                total_records_counter += 1
                # This functionality should ideally live outside of this method
                # but since state is managed inside this method, we keep track
                # of it here.
                if self._limit_reached(internal_config, total_records_counter):
                    # Break from slice loop to save state and exit from _read_incremental function.
                    break

            yield self._checkpoint_state(stream_instance, stream_state, connector_state)
            if self._limit_reached(internal_config, total_records_counter):
                return

    def _as_airbyte_record(self, stream_name: str, data: Mapping[str, Any], namespace=None):
        """
        TODO: This is a workaround until custom namespace support will be provided for Streams.
        """

        now_millis = int(datetime.now().timestamp() * 1000)
        transformer, schema = self._get_stream_transformer_and_schema(stream_name)
        # Transform object fields according to config. Most likely you will
        # need it to normalize values against json schema. By default no action
        # taken unless configured. See
        # docs/connector-development/cdk-python/schemas.md for details.
        transformer.transform(data, schema)  # type: ignore

        # TODO: Q&D workaound for custom namespace support
        message = AirbyteRecordMessage(stream=stream_name, data=data, emitted_at=now_millis, namespace=namespace)
        
        return AirbyteMessage(type=MessageType.RECORD, record=message)