# -*- coding: utf-8 -*-
import typing

from kiara.data.values import ValueSchema
from kiara.data.values.value_set import ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module import KiaraModule
from kiara.pipeline import StepStatus
from kiara.pipeline.config import PipelineConfig
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
        # controller: typing.Union[
        #     None, PipelineController, str, typing.Type[PipelineController]
        # ] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        # if controller is not None and not isinstance(controller, PipelineController):
        #     raise NotImplementedError()
        # if controller is None:
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

        # controller = BatchController(auto_process=False, kiara=self._kiara)

        pipeline = Pipeline(structure=self.structure)
        pipeline.inputs.set_values(**inputs)

        if not pipeline.inputs.items_are_valid():
            raise KiaraProcessingException(f"Can't start processing of {self._module_type_id} pipeline: one or several inputs missing or invalid.")  # type: ignore

        if not pipeline.status == StepStatus.RESULTS_READY:
            # TODO: error details
            raise KiaraProcessingException(f"Error when running pipeline of type '{self._module_type_id}'.")  # type: ignore

        outputs.set_values(**pipeline.outputs)
