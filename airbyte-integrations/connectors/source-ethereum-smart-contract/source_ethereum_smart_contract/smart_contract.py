import json
import logging
from datetime import datetime
from web3 import Web3

class Web3Connection(object):

    TIMEOUT = 100

    def __init__(self, connection):
        self.__connection = connection
        self._web3_provider = None
        self._web3_connection = None

    @property
    def host(self):
        if 'host' not in self.__connection:
            raise Exception('No host information found!')

        return self.__connection['host']

    @property
    def web3_provider(self):
        if self._web3_provider is None:
            if 'connection_type' not in self.__connection:
                raise Exception("No connection type information found ['http', 'ws']!")

            if self.__connection['connection_type'] == 'http':
                self._web3_provider = Web3.HTTPProvider(endpoint_uri=self.host, request_kwargs={'timeout': self.TIMEOUT})
            elif self.__connection['connection_type'] == 'ws':
                self._web3_provider = Web3.WebsocketProvider(endpoint_uri=self.host, websocket_timeout=self.TIMEOUT)
            else:
                raise Exception("No valid connection type found ['http', 'ws']!")
        
        return self._web3_provider

    @property
    def web3_connection(self):
        if self._web3_connection is None:
            self._web3_connection = Web3(self.web3_provider)
        
        return self._web3_connection


class SmartContract(object):
    
    CHUNK_OF_BLOCKS = 2000
    
    def __init__(self, connection, smart_contract_address, smart_contract_abi, smart_contract_deployed_at_block=0, logger=None):
        # w3 = Web3Connection(connection)
        self._web3_connection = Web3Connection(connection).web3_connection
        self.__smart_contract = None
        self.__smart_contract_address = smart_contract_address
        self.__smart_contract_abi = smart_contract_abi
        self.__smart_contract_deployed_at_block = smart_contract_deployed_at_block
        self.__logger = logger
        self.__name = None
        self.__symbol = None

    def check_connection(self) -> bool:
        isConnected = self._web3_connection.isConnected()

        # check connection to ETH node
        if not isConnected:
            self.logger.error("Could not connect to Ethereum node!")
            return False

        blocknumber = self.latest_block

        # check syncing state of ETH node
        if type(blocknumber) is not int:
            self.logger.error("Could not retrieve current block height!")
            return False

        self.logger.info(f"Connection [{isConnected}] with latest block [{blocknumber}]")

        return True

    def check_smart_contract_address(self) -> bool:
        # check correct address format - checksum
        correct_smart_contract_address = self._web3_connection.isAddress(self.__smart_contract_address)

        if not correct_smart_contract_address:
            self.logger.error("Entered address is not correct ['{address}']!".format(address=self.__smart_contract_address))
            return False

        self.logger.info("Entered address ['{address}'] is correct.".format(address=self.__smart_contract_address))

        return True
    
    def check_abi_file(self) -> bool:
        smart_contract_abi = json.loads(self.__smart_contract_abi)

        has_events = False

        # check ABI file
        if len(smart_contract_abi) > 0:
            for record in smart_contract_abi:
                if 'type' in record and record['type'] == 'event':
                    has_events = True
                    continue
        else:
            self.logger.error("ABI file does not have any entries!")
            return False

        if not has_events:
            self.logger.error("ABI file does not have any entries!")
            return False

        self.logger.info("Entered abi file is correct.")

        return True

    @property
    def logger(self):
        if self.__logger is None:
            self.__logger = logging.getLogger('SmartContract')
        return self.__logger

    @logger.setter
    def logger(self, logger):
        self.__logger = logger

    @property
    def deployed_at_block(self):
        return self.__smart_contract_deployed_at_block

    @property
    def latest_block(self):
        return self._web3_connection.eth.blockNumber

    @property
    def smart_contract(self):
        if self.__smart_contract is None:
            self.__smart_contract = self._web3_connection.eth.contract(address=self.__smart_contract_address, abi=self.__smart_contract_abi)
        return self.__smart_contract
    
    @property
    def name(self):
        if self.__name is None:
            try:
                self.__name = self.smart_contract.functions.name().call()
            except Exception as exc:
                self.logger.error("Could not retrieve name: {exc}".format(exc=exc))
        return self.__name
    
    @property
    def symbol(self):
        if self.__symbol is None:
            try:
                self.__symbol = self.smart_contract.functions.symbol().call()
            except Exception as exc:
                self.logger.error("Could not retrieve symbol: {exc}".format(exc=exc))
        return self.__symbol

    @staticmethod
    def convert_unix_time_to_date_time(unix_timestamp):
        return datetime.fromtimestamp(unix_timestamp).strftime("%Y-%m-%d %H:%M:%S")
    
    def get_event_from_abi(self, event_name):
        abi = self.smart_contract.events.abi
        abi_event = [entry for entry in abi if entry['type'] == 'event' and entry['name'] == event_name]
        
        if len(abi_event) > 1:
            raise Exception('Duplicate events found in ABI for event [{event_name}]'.format(event_name=event_name))
        
        if len(abi_event) == 0:
            raise Exception('No event found in ABI for event [{event_name}]'.format(event_name=event_name))
        
        return abi_event[0]
    
    def get_all_events_from_abi(self):
        return [entry['name'] for entry in self.smart_contract.events.abi if entry['type'] == 'event']
    
    def get_events(self, event_name, from_block=None, to_block='latest') -> tuple:
        from_block=self.__smart_contract_deployed_at_block if from_block is None else from_block
        
        # TODO: Q&D get timestamp function added inline
        def get_timestamp(self, b):
            if b in timestamps:
                return timestamps[b]
            else:
                unix_time = self._web3_connection.eth.getBlock(b).timestamp
                timestamps[b] = self.convert_unix_time_to_date_time(unix_time)
                return timestamps[b]
        
        to_block = self.latest_block if to_block == 'latest' else to_block
        
        event = getattr(self.smart_contract.events, event_name)

        timestamps = dict()
        events = list()
        
        for b in range(from_block, (to_block // self.CHUNK_OF_BLOCKS + 1) * self.CHUNK_OF_BLOCKS, self.CHUNK_OF_BLOCKS):
            
            self.logger.info('Fetching {event} for {symbol} [{from_block}/{to_block}]'.format(
                                                                                        event=event_name, 
                                                                                        from_block=b, 
                                                                                        to_block=b + self.CHUNK_OF_BLOCKS, 
                                                                                        symbol=self.symbol
                                                                                        )
                        )
            
            response = [dict(
                ev.args, 
                **{
                    'block': ev.blockNumber,
                    'timestamp': get_timestamp(self, ev.blockNumber),
                    'smart_contract_address': self.__smart_contract_address,
                    'symbol': self.symbol if self.symbol else self.name
                }
            ) for ev in event.getLogs(fromBlock=b, toBlock=b + self.CHUNK_OF_BLOCKS)]

            events.extend(response)
        
        return events
        