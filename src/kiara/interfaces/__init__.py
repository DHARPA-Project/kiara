# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Implementation of interfaces for *Kiara*."""
import contextlib
import os
import sys
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    Literal,
    Union,
)

from kiara.exceptions import KiaraException
from kiara.utils.cli import terminal_print

if TYPE_CHECKING:
    from rich.console import Console

    from kiara.context import Kiara
    from kiara.context.config import KiaraConfig
    from kiara.interfaces.python_api.base_api import BaseAPI

# log = structlog.getLogger()

# Global console used by alternative print
_console: Union["Console", None] = None


def create_console(
    width: Union[None, int],
    color_system: Union[
        None, Literal["auto", "standard", "256", "truecolor", "windows"]
    ] = None,
) -> "Console":
    """Create a console instance."""
    from rich.console import COLOR_SYSTEMS
    from rich.highlighter import RegexHighlighter
    from rich.theme import Theme

    STYLE_OPTION = "bold cyan"
    STYLE_ARGUMENT = "bold cyan"
    STYLE_SWITCH = "bold green"
    STYLE_METAVAR = "bold yellow"
    STYLE_METAVAR_SEPARATOR = "dim"
    STYLE_USAGE = "yellow"

    theme = Theme(
        {
            "option": STYLE_OPTION,
            "argument": STYLE_ARGUMENT,
            "switch": STYLE_SWITCH,
            "metavar": STYLE_METAVAR,
            "metavar_sep": STYLE_METAVAR_SEPARATOR,
            "usage": STYLE_USAGE,
        }
    )

    class OptionHighlighter(RegexHighlighter):
        """Highlights our special options."""

        highlights: ClassVar = [  # type: ignore
            r"(^|\W)(?P<switch>\-\w+)(?![a-zA-Z0-9])",
            r"(^|\W)(?P<option>\-\-[\w\-]+)(?![a-zA-Z0-9])",
            r"(^|\W)(?P<argument>[A-Z0-9\_]+)(?![_a-zA-Z0-9])",
            r"(?P<metavar>\<[^\>]+\>)",
            r"(?P<usage>Usage: )",
        ]

    highlighter = OptionHighlighter()
    FORCE_TERMINAL = (
        True
        if os.getenv("GITHUB_ACTIONS")
        or os.getenv("FORCE_COLOR")
        or os.getenv("PY_COLORS")
        else None
    )

    console_width = None
    if width is not None:
        console_width = width
    else:
        _console_width = os.environ.get("CONSOLE_WIDTH", None)
        if _console_width:
            try:
                console_width = int(_console_width)
            except Exception:
                pass

    from rich.console import Console

    if color_system is not None:
        _color_system = color_system
    else:
        _color_system = "auto"

    if _color_system != "auto" and _color_system not in COLOR_SYSTEMS.keys():
        raise Exception(
            f"Invalid color system '{_color_system}': available options: auto, {', '.join(COLOR_SYSTEMS.keys())}."
        )

    _console = Console(
        width=console_width,
        theme=theme,
        highlighter=highlighter,
        color_system=_color_system,
        force_terminal=FORCE_TERMINAL,
    )
    return _console


def get_console() -> "Console":
    """
    Get a global Console instance.

    Returns:
    -------
        Console: A console instance.
    """
    global _console
    if _console is None:
        _console = create_console(width=None, color_system=None)

    return _console


@contextlib.contextmanager
def get_proxy_console(
    width: Union[None, int] = None,
    color_system: Union[
        None, Literal["auto", "standard", "256", "truecolor", "windows"]
    ] = None,
    restore_default_console: bool = True,
) -> Iterator["Console"]:
    """
    Get a console that proxies a remote one.

    This should only be used in a single-threaded way.
    """
    global _console

    default_console = get_console()
    old_width = default_console.width

    changed = False
    changed_width = False
    # TODO: maybe use thread-local?
    if width is None and color_system is None:
        result = default_console
    else:
        if width is None:
            width = default_console.width

        if color_system is None:
            color_system = default_console.color_system  # type: ignore

        if color_system != default_console.color_system:
            result = create_console(width=width, color_system=color_system)
            _console = result
            changed = True
        elif width != default_console.width:
            default_console.width = width
            changed_width = True
        else:
            result = default_console

    yield result

    if changed and restore_default_console:
        _console = default_console

    if changed_width and restore_default_console:
        default_console.width = old_width


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


class BaseAPIWrap(object):
    """A wrapper class to help with lazy loading.

    This is mostly relevant in terms of Python imports and the cli, because that allows
    to avoid importing lots of Python modules if only `--help` is called.
    """

    def __init__(
        self,
        config: Union[str, None],
        context: Union[str, None],
        pipelines: Union[None, Iterable[str]] = None,
        ensure_plugins: Union[str, Iterable[str], None] = None,
        exit_process: bool = True,
    ):

        if not context:
            context = os.environ.get("KIARA_CONTEXT", None)

        if context and context.endswith(".kontext") and os.path.exists(context):
            context = os.path.abspath(context)

        self._config: Union[str, None] = config
        self._context: Union[str, None] = context

        self._pipelines: Union[None, Iterable[str]] = pipelines
        self._ensure_plugins: Union[str, Iterable[str], None] = ensure_plugins

        self._kiara_config: Union["KiaraConfig", None] = None
        self._api: Union[BaseAPI, None] = None

        self._reload_process_if_plugins_installed = True

        self._items: Dict[str, Any] = {}
        self._exit_process: bool = exit_process

    @property
    def kiara_context_name(self) -> str:

        if not self._context:
            self._context = self.kiara_config.default_context

        return self._context

    @property
    def exit_process(self) -> bool:
        return self._exit_process

    @exit_process.setter
    def exit_process(self, exit_process: bool):
        self._exit_process = exit_process

    def exit(self, msg: Union[None, Any] = None, exit_code: int = 1):

        if self._exit_process:
            if msg:
                terminal_print(msg)
            sys.exit(exit_code)
        else:
            if not msg:
                msg = "An error occured."
            raise KiaraException(str(msg))

    @property
    def current_kiara_context_id(self) -> uuid.UUID:

        return self.base_api.context.id

    @property
    def kiara(self) -> "Kiara":
        return self.base_api.context

    @property
    def kiara_config(self) -> "KiaraConfig":

        if self._kiara_config is not None:
            return self._kiara_config

        try:
            from kiara.utils.config import assemble_kiara_config

            kiara_config = assemble_kiara_config(config_file=self._config)
        except Exception as e:
            terminal_print()
            terminal_print(f"Error loading kiara config: {e}")
            sys.exit(1)

        # kiara_config.runtime_config.runtime_profile = "default"

        self._kiara_config = kiara_config
        return self._kiara_config

    def lock_file(self, context: str) -> str:
        """The path to the lock file for this context."""
        return "asdf"

    @property
    def base_api(self) -> "BaseAPI":

        if self._api is not None:
            return self._api

        from kiara.utils import log_message

        context = self._context
        if not context:
            context = self.kiara_config.default_context

        from kiara.interfaces.python_api.base_api import BaseAPI

        api = BaseAPI(kiara_config=self.kiara_config)
        if self._ensure_plugins:
            installed = api.ensure_plugin_packages(self._ensure_plugins, update=False)
            if installed and self._reload_process_if_plugins_installed:
                log_message(
                    "replacing.process",
                    reason="reloading this process, in order to pick up new plugin packages",
                )
                os.execvp(sys.executable, (sys.executable,) + tuple(sys.argv))  # noqa

        import fasteners

        fasteners.InterProcessReaderWriterLock(self.lock_file(context))

        api.set_active_context(context, create=True)

        if self._pipelines:
            for pipeline in self._pipelines:
                ops = api.context.operation_registry.register_pipelines(pipeline)
                for op_id in ops.keys():
                    log_message("register.pipeline", operation_id=op_id)

        self._api = api
        return self._api

    def add_item(self, key: str, item: Any):

        self._items[key] = item

    def get_item(self, key: str) -> Any:

        if key not in self._items.keys():
            raise ValueError(f"No item with key '{key}'")

        return self._items[key]
