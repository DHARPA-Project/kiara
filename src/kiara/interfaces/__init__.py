# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Implementation of interfaces for *Kiara*."""

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Union

from kiara.defaults import KIARA_CONFIG_FILE_NAME, KIARA_MAIN_CONFIG_FILE

if TYPE_CHECKING:
    from rich.console import Console

    from kiara.context import Kiara
    from kiara.context.config import KiaraConfig
    from kiara.interfaces.python_api import KiaraAPI


# log = structlog.getLogger()

# Global console used by alternative print
_console: Union["Console", None] = None


def get_console() -> "Console":
    """Get a global Console instance.

    Returns:
        Console: A console instance.
    """
    global _console
    if _console is None:
        console_width = os.environ.get("CONSOLE_WIDTH", None)
        width = None

        if console_width:
            try:
                width = int(console_width)
            except Exception:
                pass

        from rich.console import Console

        _console = Console(width=width)

    return _console


def set_console_width(width: Union[int, None] = None, prefer_env: bool = True):

    global _console
    if prefer_env or not width:
        _width: Union[None, int] = None
        try:
            _width = int(os.environ.get("CONSOLE_WIDTH", None))  # type: ignore
        except Exception:
            pass
        if _width:
            width = _width

    if width:
        try:
            width = int(width)
        except Exception as e:
            import structlog

            log = structlog.getLogger()
            log.debug("invalid.console_width", error=str(e))

    from rich.console import Console

    _console = Console(width=width)

    if not width:
        if "google.colab" in sys.modules or "jupyter_client" in sys.modules:
            width = 140

    if width:
        import rich

        con = rich.get_console()
        con.width = width


class KiaraAPIWrap(object):
    def __init__(
        self,
        config: Union[str, None],
        context: Union[str, None],
        pipelines: Union[None, Iterable[str]] = None,
        ensure_plugins: Union[str, Iterable[str], None] = None,
    ):

        if not context:
            context = os.environ.get("KIARA_CONTEXT", None)

        self._config: Union[str, None] = config
        self._context: Union[str, None] = context

        self._pipelines: Union[None, Iterable[str]] = pipelines
        self._ensure_plugins: Union[str, Iterable[str], None] = ensure_plugins

        self._kiara_config: Union["KiaraConfig", None] = None
        self._api: Union[KiaraAPI, None] = None

        self._reload_process_if_plugins_installed = True

    @property
    def kiara_context_name(self) -> str:

        context = self._context
        if not context:
            context = self.kiara_config.default_context
        return context

    @property
    def kiara(self) -> "Kiara":
        return self.kiara_api.context

    @property
    def kiara_config(self) -> "KiaraConfig":

        if self._kiara_config is not None:
            return self._kiara_config
        from kiara.context.config import KiaraConfig

        # kiara_config: Optional[KiaraConfig] = None
        exists = False
        create = False
        if self._config:
            config_path = Path(self._config)
            if config_path.exists():
                if config_path.is_file():
                    config_file_path = config_path
                    exists = True
                else:
                    config_file_path = config_path / KIARA_CONFIG_FILE_NAME
                    if config_file_path.exists():
                        exists = True
            else:
                config_path.parent.mkdir(parents=True, exist_ok=True)
                config_file_path = config_path

        else:
            config_file_path = Path(KIARA_MAIN_CONFIG_FILE)
            if not config_file_path.exists():
                create = True
                exists = False
            else:
                exists = True

        if not exists:
            if not create:
                from kiara.utils.cli import terminal_print

                terminal_print()
                terminal_print(
                    f"Can't create kiara context, specified config file does not exist: {self._config}."
                )
                sys.exit(1)

            kiara_config = KiaraConfig()
            kiara_config.save(config_file_path)
        else:
            kiara_config = KiaraConfig.load_from_file(config_file_path)

        self._kiara_config = kiara_config
        return self._kiara_config

    @property
    def kiara_api(self) -> "KiaraAPI":

        if self._api is not None:
            return self._api

        from kiara.utils import log_message

        context = self._context
        if not context:
            context = self.kiara_config.default_context

        from kiara.interfaces.python_api import KiaraAPI

        api = KiaraAPI(kiara_config=self.kiara_config)
        if self._ensure_plugins:
            installed = api.ensure_plugin_packages(self._ensure_plugins, update=False)
            if installed and self._reload_process_if_plugins_installed:
                log_message(
                    "replacing.process",
                    reason="reloading this process, in order to pick up new plugin packages",
                )
                os.execvp(sys.executable, (sys.executable,) + tuple(sys.argv))  # noqa

        api.set_active_context(context, create=True)

        if self._pipelines:
            for pipeline in self._pipelines:
                ops = api.context.operation_registry.register_pipelines(pipeline)
                for op_id in ops.keys():
                    log_message("register.pipeline", operation_id=op_id)

        self._api = api
        return self._api
