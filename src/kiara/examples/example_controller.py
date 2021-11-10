# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

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
        and_job_id = self.process_step("and")
        self.wait_for_jobs(and_job_id)
        not_job_id = self.process_step("not")
        self.wait_for_jobs(not_job_id)


def execute_pipeline_with_example_controller():

    kiara = Kiara.instance()
    controller = ExampleController()
    workflow = kiara.create_workflow("logic.nand", controller=controller)

    workflow.inputs.set_value("a", True)
    workflow.inputs.set_value("b", False)

    controller.execute()

    print("Pipeline result:")
    print(workflow.outputs.get_all_value_data())
