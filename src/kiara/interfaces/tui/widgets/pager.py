# -*- coding: utf-8 -*-
from rich import box
from rich.console import RenderableType
from rich.table import Table
from rich.text import Text
from rich.tree import Tree
from textual.reactive import reactive
from textual.widgets import Static
from typing import Dict, Iterable, List, Mapping, Tuple, Union

from kiara.models.rendering import RenderScene


class DataViewPane(Static):
    """A widget that displays a data preview."""

    value_view = reactive(None)
    # is_scrollable = True

    def __init__(self, *args, **kwargs) -> None:

        super().__init__(*args, **kwargs)

    def watch_value_view(self, value_view: Union[RenderableType, None]) -> None:
        """Update the data preview."""

        if not value_view:
            self.update("-- no value --")
        else:
            self.update(value_view)


class DataViewControl(Static):

    current_value: Union[str, None] = None
    related_scenes: Union[None, Mapping[str, Union[None, RenderScene]]] = None
    scene_keys: Union[None, Dict[str, Union[None, RenderScene]]] = None
    max_level: int = 0
    control_table: Union[None, RenderableType] = None
    reserved_keys: Tuple[str, ...] = ()

    def on_mount(self):

        self.compute_related_scenes(None)
        self.commit()

    def set_reserved_keys(self, keys: Iterable[str]):
        self.reserved_keys = tuple(keys)

    def get_title(
        self,
        key: str,
        scene: Union[None, RenderScene],
        scene_keys: Dict[str, Union[None, RenderScene]],
    ) -> RenderableType:

        last_token = key.split(".")[-1]

        title = None
        found_key = None
        for idx, command_key in enumerate(last_token):
            if (
                command_key.lower() not in self.reserved_keys
                and command_key.lower() not in (x.lower() for x in scene_keys.keys())
            ):
                replaced = last_token.replace(
                    command_key, f"\[{command_key}]", 1  # noqa
                )  # noqa
                if scene is None or scene.disabled:
                    title = Text.from_markup(f"[grey46]{replaced}[/grey46]")
                else:
                    title = Text.from_markup(replaced)  # noqa
                found_key = command_key
                break

        if title is None:
            raise NotImplementedError("Could not find free command key.")
        if found_key is None:
            raise NotImplementedError("Invalid key.")

        scene_keys[found_key] = scene  # tpye: ignore
        return title

    def render_sub_command_table(
        self,
        scene: Union[None, RenderScene],
        scene_keys: Dict[str, Union[None, RenderScene]],
        forced_titles: Dict[str, RenderableType],
        row_list: Union[None, List[List[RenderableType]]] = None,
        level: int = 0,
    ) -> Tuple[List[List[RenderableType]], int]:

        if row_list is None:
            row_list = []

        if not scene:
            return (row_list, level)

        if not scene.related_scenes:
            return (row_list, level)

        level = level + 1
        sub_list: List[RenderableType] = []

        for scene_key, sub_scene in scene.related_scenes.items():

            if scene_key in forced_titles.keys():
                new_scene_key: RenderableType = forced_titles[scene_key]
            else:
                new_scene_key = self.get_title(
                    key=scene_key, scene=sub_scene, scene_keys=scene_keys
                )

            sub_list.append(new_scene_key)

        row_list.append(sub_list)

        for scene_key, sub_scene in scene.related_scenes.items():
            _, level = self.render_sub_command_table(
                scene=sub_scene,
                scene_keys=scene_keys,
                forced_titles=forced_titles,
                row_list=row_list,
                level=level,
            )

        return (row_list, level)

    def render_sub_command_tree(
        self,
        key: str,
        scene: Union[RenderScene, None],
        scene_keys: Dict[str, Union[None, RenderScene]],
        forced_titles: Dict[str, RenderableType],
        node: Union[Tree, None] = None,
        level: int = 0,
    ):

        if key in forced_titles.keys():
            title: RenderableType = forced_titles[key]
        else:
            title = self.get_title(key=key, scene=scene, scene_keys=scene_keys)

        if node is None:
            node = Tree(title)
        else:
            node = node.add(title)

        if scene:
            for scene_key, sub_scene in scene.related_scenes.items():
                _, level = self.render_sub_command_tree(
                    key=f"{key}.{scene_key}",
                    scene=sub_scene,
                    scene_keys=scene_keys,
                    forced_titles=forced_titles,
                    node=node,
                    level=level + 1,
                )

        return (node, level)

    def render_command_table(self) -> Tuple[RenderableType, int]:

        max_level = 0

        if self.related_scenes is None:
            return "", max_level

        scene_keys: Dict[str, Union[None, RenderScene]] = {}

        forced_titles = {}
        for key, scene in self.related_scenes.items():

            title = self.get_title(key=key, scene=scene, scene_keys=scene_keys)
            forced_titles[key] = title

        render_as_tree = False
        if render_as_tree:
            row = []
            table = Table(show_header=False, box=box.SIMPLE)
            for key, scene in self.related_scenes.items():

                table.add_column(f"category: {key}")
                node, level = self.render_sub_command_tree(
                    key=key,
                    scene=scene,
                    scene_keys=scene_keys,
                    forced_titles=forced_titles,
                )
                row.append(node)

                control_height = ((level * 2) - 1) + 2
                if control_height > max_level:
                    max_level = control_height

            table.add_row(*row)

        else:
            main_scene = RenderScene(
                title=f"Value: {self.current_value}",
                disabled=False,
                description=f"Preview of value: {self.current_value}",
                manifest_hash="",
                render_config={},
                related_scenes=self.related_scenes,
            )
            row_list, level = self.render_sub_command_table(
                scene=main_scene,
                scene_keys=scene_keys,
                forced_titles=forced_titles,
            )

            table = Table(show_header=False, box=box.SIMPLE, show_lines=True)
            if not row_list:
                max_level = 0
            else:
                max_len = max([len(x) for x in row_list])
                for i in range(0, max_len):
                    table.add_column(f"col_{i}")

                for row in row_list:
                    table.add_row(*row)

            if level > max_level:
                max_level = level

        self.scene_keys = scene_keys
        return table, max_level

    def compute_related_scenes(
        self, related_scenes: Union[None, Mapping[str, Union[None, RenderScene]]]
    ) -> int:

        self.related_scenes = related_scenes
        self.control_table, self.max_level = self.render_command_table()

        return self.max_level

    def commit(self):

        self.update(self.control_table)

    def get_scene_for_key(self, key: str) -> Union[None, RenderScene]:

        if self.scene_keys is None:
            return None

        if key in self.scene_keys.keys():
            return self.scene_keys[key]

        for x in self.scene_keys.keys():
            if x.lower() == key.lower():
                return self.scene_keys[x]

        return None


# class DataPreview(Static):
#
#     CSS_PATH = os.path.join(KIARA_RESOURCES_FOLDER, "tui", "pager_app.css")
#
#     # BINDINGS = [("v", "send_command('next')", "Next page"), ("p", "send_command('previous')", "Previous page")]
#     can_focus = True
#     is_scrollable = True
#
#     class Command(Message):
#         """Color selected message."""
#
#         def __init__(self, sender: MessageTarget, command: str) -> None:
#             self.command = command
#             super().__init__(sender)
#
#     def __init__(
#         self,
#         api: KiaraAPI,
#         base_id: str,
#         value: Union[None, str] = None,
#         *args,
#         **kwargs,
#     ):
#
#         self._api: KiaraAPI = api
#         self._current_value: Union[None, str] = value
#
#         self._current_render_config: Union[Mapping[str, Any], None] = None  # type: ignore
#         self._current_result: Union[RenderValueResult, None] = None  # type: ignore
#         self._value_preview = DataViewPane()
#
#         self._base_id = base_id
#         self._preview = DataViewPane(id=f"{base_id}.preview")
#         self._control = DataViewControl(id=f"{base_id}.control")
#
#         super().__init__(*args, id=self._base_id, **kwargs)
#
#     @property
#     def control_height(self) -> int:
#         return self._control_height
#
#     def set_num_rows(self, num_rows: int):
#
#         if self._num_rows == num_rows:
#             return
#
#         self._num_rows = num_rows
#         # self._preview.styles.height = self._num_rows + 4
#         # self.styles.height = (self._num_rows + 4) + self._control_height + 2
#         self.update_scene(self._current_render_config)
#
#     def on_mount(self):
#
#         self._control.compute_related_scenes(None)
#         self._control.commit()
#         self._preview.value_view = "initializing..."
#
#     def set_value(self, value: Union[None, str]) -> None:
#
#         self._current_value = value
#         self.update_scene({})
#
#     def on_key(self, event: events.Key) -> None:
#
#         scene = self._control.get_scene_for_key(event.key)
#         if scene:
#             self.update_scene(scene.render_config)
#
#     def compose(self) -> ComposeResult:
#
#         yield self._preview
#         yield self._control
#
#     def update_scene(self, render_config: Union[None, Mapping[str, Any]]):
#
#         if not self._current_value:
#             self._preview.renderable = "-- no value --"
#             self._control.compute_related_scenes(None)
#             self._control.commit()
#             return
#
#         if render_config is None:
#             return
#
#         if self._num_rows <= 0:
#             self._num_rows = 4
#
#         rc = dict(render_config)
#         rc["number_of_rows"] = self._num_rows
#
#         render_result = self._api.render_value(
#             value=self._current_value,
#             target_format="terminal_renderable",
#             render_config=rc,
#         )
#
#         max_level = self._control.compute_related_scenes(render_result.related_scenes)
#
#         if max_level != self._control_height:
#             self._control_height = max_level
#             self.set_num_rows(self._num_rows - (self._control_height))
#             self.update_scene(render_config=rc)
#         else:
#
#             self._current_render_config = rc
#             self._current_result = render_result
#             self._preview.value_view = render_result.rendered
#             self._control.commit()
