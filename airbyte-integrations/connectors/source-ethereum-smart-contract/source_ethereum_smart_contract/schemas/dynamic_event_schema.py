from airbyte_cdk.models import AirbyteStream
from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode, SyncMode

class DynamicEventSchema(object):
    
    # map abi data types to sql data types
    DATA_TYPE_MAPPING = {
        'int24':  {"type": "string", "airbyte_type": "big_integer"},
        'int128': {"type": "string", "airbyte_type": "big_integer"},
        'int256': {"type": "string", "airbyte_type": "big_integer"},
        'uint8':  {"type": "string", "airbyte_type": "big_integer"},
        'uint16': {"type": "string", "airbyte_type": "big_integer"},
        'uint112': {"type": "string", "airbyte_type": "big_integer"},
        'uint128': {"type": "string", "airbyte_type": "big_integer"},
        'uint160': {"type": "string", "airbyte_type": "big_integer"},
        'uint256': {"type": "string", "airbyte_type": "big_integer"},
        'uint256[2]': {"type": ["null", "array"] },
        'address': {"type": ["null", "string"] }
    }
    
    def __init__(self, namespace: str, event_abi: dict):
        self.__namespace = namespace
        self.__event_abi = event_abi
    
    @property
    def name(self) -> str:
        event = self.__event_abi

        if 'name' not in event:
            raise Exception("No name provided by event {event}}]".format(event=event))

        return event['name']

    @property
    def primary_key(self) -> AirbyteStream:
        return self.schema["properties"].keys()

    @property
    def stream(self) -> AirbyteStream:
        return AirbyteStream(
            name=self.name, 
            json_schema=self.schema, 
            supported_sync_modes=[SyncMode.incremental],
            supported_destination_sync_modes=[DestinationSyncMode.append_dedup],
            namespace=self.__namespace,
            default_cursor_field=['block'])

    @property
    def schema(self) -> dict:
        event = self.__event_abi
        
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "block": { "type": "integer" },
                "timestamp": { "type": "string", "format": "datetime" },
                "smart_contract_address": { "type": "string" },
                "symbol": { "type": ["null", "string"] }
                }
            }
        
        if 'inputs' in event:
            # add new column for each input to table attributes
            for input in event['inputs']:
                column_name = input['name']
                data_type = input['type']
                # indexed = convert_str_to_bool(input['indexed'])
                
                if data_type not in self.DATA_TYPE_MAPPING:
                    raise Exception('ABI data type [{abi_data_type}] not covered in data type mapper!'.format(abi_data_type=data_type))
                
                json_schema["properties"][column_name] =  self.DATA_TYPE_MAPPING[data_type]
                
        else:
            raise Exception("No inputs provided by event {event}}]".format(event=event))
        
        return json_schema
