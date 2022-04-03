import uuid
from typing import Dict, Any, Optional
import structlog
from kiara.utils import is_debug, is_develop

logger = structlog.getLogger()

class IdRegistry(object):
    def __init__(self):
        self._ids: Dict[uuid.UUID, Any] = {}

    def generate(self, id: Optional[uuid.UUID] = None, **metadata: Any):

        if id is None:
            id = uuid.uuid4()
        if is_debug() or is_develop():

            obj = metadata.pop("obj", None)
            # TODO: store this in a weakref dict
            logger.debug("generate.id", id=id, metadata=metadata)
            self._ids.setdefault(id, []).append(metadata)

        return id


ID_REGISTRY = IdRegistry()
