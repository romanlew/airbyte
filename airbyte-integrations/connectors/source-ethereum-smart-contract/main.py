#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_ethereum_smart_contract import SourceEthereumSmartContract

if __name__ == "__main__":
    source = SourceEthereumSmartContract()
    launch(source, sys.argv[1:])
