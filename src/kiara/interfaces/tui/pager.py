# -*- coding: utf-8 -*-
import structlog
from textual import events
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import Footer, Header
from typing import Any, Mapping, Union

from kiara.interfaces import get_console
from kiara.interfaces.python_api import KiaraAPI
from kiara.interfaces.tui.widgets.pager import DataViewControl, DataViewPane
from kiara.models.rendering import RenderValueResult

logger = structlog.getLogger()

RESERVED_KEYS = ("q", "r")


class PagerApp(App):

    CSS = """
DataViewPane {
}
DataViewControl {
    dock: bottom;
    padding: 1 0;
}
"""
    BINDINGS = [Binding(key="q", action="quit", description="Quit")]

    def __init__(
        self,
        api: Union[None, KiaraAPI] = None,
        value: Union[str, None] = None,
        *args,
        **kwargs,
    ):

        if api is None:
            api = KiaraAPI.instance()

        self._base_id = "data_preview"

        self._api: KiaraAPI = api

        self._init_value: Union[None, str] = value

        self._current_value: Union[None, str] = value

        self._current_render_config: Union[Mapping[str, Any], None] = None  # type: ignore
        self._current_result: Union[RenderValueResult, None] = None  # type: ignore

        self._data_preview = DataViewPane(id=f"{self._base_id}.preview")
        self._preview_control = DataViewControl(id=f"{self._base_id}.control")
        self._preview_control.set_reserved_keys(RESERVED_KEYS)

        self._num_rows: int = 0
        self._control_height = 0

        super().__init__(*args, **kwargs)

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""

        yield Header()
        yield self._data_preview
        yield self._preview_control
        yield Footer()

    def on_mount(self) -> None:

        self._current_value = self._init_value
        self.update_scene()

    def action_redraw_preview(self) -> None:

        self.update_scene()

    def recalculate_num_rows(self) -> None:

        if self._control_height == 0:
            ch = 0
        else:
            ch = self._control_height + 2

        self._num_rows = (get_console().size.height - 8) - ch

    def on_key(self, event: events.Key) -> None:

        if event.key == "r":
            self.action_redraw_preview()
            return

        scene = self._preview_control.get_scene_for_key(event.key)
        if scene:
            self._current_render_config = scene.render_config
            self.update_scene()

    def update_scene(self):

        if not self._current_value:
            self._control_height = self._preview_control.compute_related_scenes(None)
            self._preview_control.commit()
            self._data_preview.value_view = "-- no value --"
            return

        self.recalculate_num_rows()
        if self._current_render_config is None:
            self._current_render_config = {}
        self._current_render_config["number_of_rows"] = self._num_rows
        self._current_render_config["display_width"] = get_console().size.width - 4

        current_result = self._api.render_value(
            value=self._current_value,
            target_format="terminal_renderable",
            render_config=self._current_render_config,
        )
        control_height = self._preview_control.compute_related_scenes(
            current_result.related_scenes
        )

        if control_height != self._control_height:
            self._control_height = control_height
            self.update_scene()
        else:
            self._current_result = current_result
            self._data_preview.value_view = current_result.rendered
            self._preview_control.commit()


if __name__ == "__main__":
    app = PagerApp(value="journals_node.table")
    app.run()
