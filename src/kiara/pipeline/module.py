# -*- coding: utf-8 -*-
import typing

from kiara.data.values import ValueSchema
from kiara.data.values.value_set import ValueSet
from kiara.module import KiaraModule
from kiara.pipeline.config import PipelineConfig
from kiara.pipeline.controller import PipelineController
from kiara.pipeline.structure import PipelineStructure
from kiara.utils import StringYAML

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara

yaml = StringYAML()


class PipelineModule(KiaraModule[PipelineConfig]):
    """A [KiaraModule][kiara.module.KiaraModule] that contains a collection of interconnected other modules."""

    _config_cls: typing.Type[PipelineConfig] = PipelineConfig  # type: ignore
    _module_type_id = "pipeline"

    @classmethod
    def is_pipeline(cls) -> bool:
        return True

    def __init__(
        self,
        id: typing.Optional[str],
        parent_id: typing.Optional[str] = None,
        module_config: typing.Union[
            None, PipelineConfig, typing.Mapping[str, typing.Any]
        ] = None,
        controller: typing.Union[
            None, PipelineController, str, typing.Type[PipelineController]
        ] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        if controller is not None and not isinstance(controller, PipelineController):
            raise NotImplementedError()
        if controller is None:
            from kiara.pipeline.controller.batch import BatchController

            controller = BatchController(kiara=kiara)

        self._pipeline_controller: PipelineController = controller
        super().__init__(
            id=id,
            parent_id=parent_id,
            module_config=module_config,
            kiara=kiara,
        )
        self._pipeline_structure: PipelineStructure = self._create_structure()
        assert not self._config.constants
        self._config.constants = dict(self._pipeline_structure.constants)

    @property
    def structure(self) -> PipelineStructure:
        """The ``PipelineStructure`` of this module."""

        return self._pipeline_structure

    def _create_structure(self) -> PipelineStructure:

        pipeline_structure = PipelineStructure(config=self.config, kiara=self._kiara)
        return pipeline_structure

    def create_input_schema(self) -> typing.Mapping[str, ValueSchema]:
        return self.structure.pipeline_input_schema

    def create_output_schema(self) -> typing.Mapping[str, ValueSchema]:
        return self.structure.pipeline_output_schema

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        from kiara import Pipeline

        # TODO: check for Value objects
        pipeline = Pipeline(structure=self.structure)
        inps = inputs.get_all_value_data()
        pipeline.inputs.set_values(**inps)

        outputs.set_values(**pipeline.outputs.get_all_value_data())
