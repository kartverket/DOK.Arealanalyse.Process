import pytest
from pygeoapi.process.base import ProcessorExecuteError
from dokanalyse.processor import DokanalyseProcessor


def test_invalid_payload_is_rejected_with_reason():
    processor = DokanalyseProcessor({'name': 'dokanalyse'})

    with pytest.raises(ProcessorExecuteError, match='inputGeometry'):
        processor.execute({'requestedBuffer': 10})
