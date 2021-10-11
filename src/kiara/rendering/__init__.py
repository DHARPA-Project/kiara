# -*- coding: utf-8 -*-
import abc
import typing

if typing.TYPE_CHECKING:
    from kiara import Kiara


class KiaraRenderer(abc.ABC):
    def __init__(
        self,
        config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        if kiara is None:
            from kiara import Kiara

            kiara = Kiara.instance()

        self._kiara: Kiara = kiara
        if config is None:
            config = {}
        self._config: typing.Mapping[str, typing.Any] = config

    def _augment_inputs(self, **inputs: typing.Any) -> typing.Mapping[str, typing.Any]:
        return inputs

    def _post_process(
        self, rendered: typing.Any, inputs: typing.Mapping[str, typing.Any]
    ) -> typing.Any:
        return rendered

    def render(self, **inputs: typing.Mapping[str, typing.Any]):

        augmented_inputs = self._augment_inputs(**inputs)
        rendered = self._render_template(inputs=augmented_inputs)
        post_processed = self._post_process(rendered=rendered, inputs=augmented_inputs)
        return post_processed

    @abc.abstractmethod
    def _render_template(self, inputs: typing.Mapping[str, typing.Any]):
        pass
