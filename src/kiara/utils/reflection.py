# -*- coding: utf-8 -*-
import inspect
from typing import Any, Callable, Dict


def extract_signature_metadata(func: Callable) -> Dict[str, Any]:

    signature = inspect.signature(func)
    result: Dict[str, Any] = {}
    for param_name, param in signature.parameters.items():
        result.setdefault("parameters", {})[param_name] = {
            "type": param.annotation,
        }
        default = None if param.default == inspect._empty else param.default
        result.setdefault("parameters", {})[param_name]["default"] = default
        result.setdefault("parameters", {})[param_name]["required"] = (
            param.default == inspect._empty
        )

    result["return_type"] = signature.return_annotation

    return result
