# -*- coding: utf-8 -*-
from rich.console import RenderableType
from textual.app import App
from textual.reactive import Reactive
from textual.widget import Widget
from typing import Any, Dict, Mapping, Union

from kiara import Kiara
from kiara.models.module.operation import Operation
from kiara.models.render_value import RenderMetadata
from kiara.models.values.value import Value


class ValuePager(Widget):

    _render_op: Operation = None  # type: ignore
    _kiara: Kiara = None  # type: ignore
    _value: Value = None  # type: ignore
    _control_widget = None  # type: ignore

    render_scene: Union[Mapping[str, Any], None] = Reactive(None)  # type: ignore

    current_rendered_value: Union[RenderableType, None] = None  # type: ignore
    current_render_metadata: Union[RenderMetadata, None] = None  # type: ignore

    def update_render_scene(self, render_metadata: Mapping[str, Any]):

        rows = self.size.height - 4
        if rows <= 0:
            rows = 1

        new_ri = dict(render_metadata)
        new_ri["number_of_rows"] = rows

        self.render_scene = new_ri

    def render(self) -> RenderableType:

        if self.render_scene is None:
            self.update_render_scene({})

        result = self._render_op.run(
            kiara=self._kiara,
            inputs={
                "value": self._value,
                "render_instruction": self.render_scene,
            },
        )

        self.current_rendered_value = result.get_value_data("rendered_value")  # type: ignore
        self.current_render_metadata = result.get_value_data("render_metadata")  # type: ignore

        self._control_widget.render_metadata = self.current_render_metadata  # type: ignore

        return self.current_rendered_value  # type: ignore


class PagerControl(Widget):

    _pager: ValuePager = None  # type: ignore
    _scene_keys: Dict[str, str] = None  # type: ignore

    render_metadata: RenderMetadata = Reactive(None)  # type: ignore

    def key_pressed(self, key: str):

        if key in self._scene_keys.keys():
            command = self._scene_keys[key]
            new_ri = self.render_metadata.related_scenes[command]
            if new_ri:
                self._pager.update_render_scene(new_ri.dict())

    def render(self) -> RenderableType:

        scene_keys: Dict[str, str] = {}
        output = []
        for key in self.render_metadata.related_scenes.keys():
            scene_keys[key[0:1]] = key
            output.append(f"\[{key[0:1]}]{key[1:]}")  # noqa: W605

        output.append("\[q]uit")  # noqa: W605

        self._scene_keys = scene_keys

        return "\n".join(output)


class PagerApp(App):
    def __init__(self, **kwargs):

        self._control = PagerControl()
        self._pager = ValuePager()

        self._pager._render_op = kwargs.pop("operation")
        self._pager._value = kwargs.pop("value")
        self._pager._kiara = kwargs.pop("kiara")
        self._pager._control_widget = self._control
        self._control._pager = self._pager

        super().__init__(**kwargs)

    # async def on_mount(self) -> None:
    #
    #     await self.view.dock(Footer(), edge="bottom")
    #     await self.view.dock(self._pager, name="data")

    async def on_mount(self) -> None:

        await self.view.dock(self._pager, self._control, edge="top")
        await self.view.dock(self._control, edge="bottom", size=10)

    async def on_load(self, event):

        await self.bind("q", "quit", "Quit")

    async def on_key(self, event):

        self._control.key_pressed(event.key)
