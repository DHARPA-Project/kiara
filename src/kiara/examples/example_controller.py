# -*- coding: utf-8 -*-
# type: ignore

from kiara import Kiara, PipelineController
from kiara.events import (
    PipelineInputEvent,
    PipelineOutputEvent,
    StepInputEvent,
    StepOutputEvent,
)


class ExampleController(PipelineController):
    def pipeline_inputs_changed(self, event: PipelineInputEvent):
        print(f"Pipeline inputs changed: {event.updated_pipeline_inputs}")
        print(f"  -> pipeline status: {self.pipeline_status.name}")

    def pipeline_outputs_changed(self, event: PipelineOutputEvent):
        print(f"Pipeline outputs changed: {event.updated_pipeline_outputs}")
        print(f"  -> pipeline status: {self.pipeline_status.name}")

    def step_inputs_changed(self, event: StepInputEvent):
        print("Step inputs changed, new values:")
        for step_id, input_names in event.updated_step_inputs.items():
            print(f"  - step '{step_id}':")
            for name in input_names:
                new_value = self.get_step_inputs(step_id).get(name).get_value_data()
                print(f"      {name}: {new_value}")

    def step_outputs_changed(self, event: StepOutputEvent):
        print("Step outputs changed, new values:")
        for step_id, output_names in event.updated_step_outputs.items():
            print(f"  - step '{step_id}':")
            for name in output_names:
                new_value = self.get_step_outputs(step_id).get(name).get_value_data()
                print(f"      {name}: {new_value}")

    def execute(self):

        print("Executing steps: 'and', 'not'...")
        self.process_step("and")
        self.process_step("not")


def execute_pipeline_with_example_controller():

    kiara = Kiara.instance()
    controller = ExampleController()
    workflow = kiara.create_workflow("nand", controller=controller)

    # note, outside of example code it's recommended to use
    # the 'workflow.inputs.set_values(...)' method to set
    # multiple values at the same time, because in most cases
    # that will be more efficient
    workflow.inputs.a = True
    workflow.inputs.b = False

    controller.execute()

    print("Pipeline result:")
    print(workflow.outputs.dict())
