# -*- coding: utf-8 -*-
import structlog
from rich import box
from rich.console import RenderableType
from rich.table import Table
from rich.tree import Tree
from textual.app import App
from textual.reactive import Reactive
from textual.widget import Widget
from typing import Any, Dict, Mapping, Union

from kiara.interfaces.python_api import KiaraAPI
from kiara.models.module.operation import Operation
from kiara.models.rendering import RenderScene, RenderValueResult
from kiara.models.values.value import Value

logger = structlog.getLogger()


class ValuePager(Widget):

    _render_op: Operation = None  # type: ignore
    _kiara_api: KiaraAPI = None  # type: ignore
    _value: Value = None  # type: ignore
    _control_widget: "PagerControl" = None  # type: ignore

    render_scene: Union[Mapping[str, Any], None] = Reactive(None)  # type: ignore

    current_result: Union[RenderValueResult, None] = None  # type: ignore

    def get_num_rows(self) -> int:
        rows = self.size.height - 5
        if rows <= 0:
            rows = 1
        return rows

    def update_render_scene(self, new_render_scene: Mapping[str, Any]):

        new_ri = dict(new_render_scene)
        new_ri.setdefault("render_config", {})["number_of_rows"] = self.get_num_rows()

        self.render_scene = new_ri

    def render(self) -> RenderableType:

        if self._value is None or not self._value.is_set:
            return "-- no value --"

        render_scene: Dict[str, Any] = self.render_scene  # type: ignore

        if render_scene is None:
            render_scene = {"render_config": {"number_of_rows": self.get_num_rows()}}
        render_result = self._kiara_api.render_value(
            value=self._value,
            target_format="terminal_renderable",
            render_config=render_scene["render_config"],
        )

        self.current_result = render_result
        self._control_widget.current_result = self.current_result

        return self.current_result.rendered  # type: ignore


class PagerControl(Widget):

    _pager: ValuePager = None  # type: ignore
    _scene_keys: Dict[str, Union[None, RenderScene]] = None  # type: ignore

    current_result: RenderValueResult = Reactive(None)  # type: ignore

    def key_pressed(self, key: str):

        if key in self._scene_keys.keys():
            new_ri = self._scene_keys.get(key)
            if new_ri:
                self._pager.update_render_scene(new_ri.dict())
        else:
            self.log(f"No matching scene for key: {key}")

    def get_title(
        self,
        key: str,
        scene: Union[None, RenderScene],
        scene_keys: Dict[str, Union[None, RenderScene]],
    ):

        last_token = key.split(".")[-1]

        title = None
        for idx, command_key in enumerate(last_token):
            if command_key not in scene_keys.keys():
                title = last_token.replace(command_key, f"\[{command_key}]", 1)  # noqa
                break

        if title is None:
            raise NotImplementedError("Could not find free command key.")

        scene_keys[command_key] = scene
        if scene is None or scene.disabled:
            title = f"[grey46]{title}[/grey46]"

        return title

    def render_sub_command_tree(
        self,
        key: str,
        scene: Union[RenderScene, None],
        scene_keys: Dict[str, Union[None, RenderScene]],
        forced_titles: Dict[str, str],
        node: Union[Tree] = None,
    ):

        if key in forced_titles.keys():
            title = forced_titles[key]
        else:
            title = self.get_title(key=key, scene=scene, scene_keys=scene_keys)

        if node is None:
            node = Tree(title)
        else:
            node = node.add(title)

        if scene:
            for scene_key, sub_scene in scene.related_scenes.items():
                self.render_sub_command_tree(
                    key=f"{key}.{scene_key}",
                    scene=sub_scene,
                    scene_keys=scene_keys,
                    forced_titles=forced_titles,
                    node=node,
                )

        return node

    def render(self) -> RenderableType:

        if self.current_result is None:
            return "-- no value --"

        scene_keys: Dict[str, Union[None, RenderScene]] = {}
        table = Table(show_header=False, box=box.SIMPLE)
        row = []

        forced_titles = {}
        for key, scene in self.current_result.related_scenes.items():

            title = self.get_title(key=key, scene=scene, scene_keys=scene_keys)
            forced_titles[key] = title

        for key, scene in self.current_result.related_scenes.items():

            table.add_column(f"category: {key}")
            row.append(
                self.render_sub_command_tree(
                    key=key,
                    scene=scene,
                    scene_keys=scene_keys,
                    forced_titles=forced_titles,
                )
            )

        table.add_row(*row)

        self._scene_keys = scene_keys
        return table


class PagerApp(App):
    def __init__(self, **kwargs):

        self._control = PagerControl()
        self._pager = ValuePager()

        self._pager._render_op = kwargs.pop("operation")
        self._pager._value = kwargs.pop("value")
        self._pager._kiara_api = kwargs.pop("kiara_api")
        self._pager._control_widget = self._control
        self._control._pager = self._pager

        super().__init__(**kwargs)

    # async def on_mount(self) -> None:
    #
    #     await self.view.dock(Footer(), edge="bottom")
    #     await self.view.dock(self._pager, name="data")

    async def on_mount(self) -> None:

        await self.view.dock(self._control, edge="bottom", size=10)
        await self.view.dock(self._pager, edge="top")

    async def on_load(self, event):

        await self.bind("q", "quit", "Quit")

    async def on_key(self, event):

        self._control.key_pressed(event.key)
