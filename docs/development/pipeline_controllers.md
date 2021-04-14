# Pipeline controllers

!!! Note
    Currently the processing of internal steps is done synchronously. This will change soon.

In *Kiara*, one of the central classes is the [Pipeline][kiara.pipeline.pipeline.Pipeline] class. It represents
an instantiated, stateful object that contains a [PipelineStructure][kiara.pipeline.structure.PipelineStructure],
as well as [Value][kiara.data.values.Value] objects for each of the 'data fields' (a.k.a. inputs/outputs) for the pipeline
itself, as well as all of the containing steps (made of other *Kiara* modules).

The ``Pipeline`` object gets notified every time one of its data fields gets updated, either due to user input, or when
an internal pipeline step finished successfully. That is when usually some action needs to happen, what action that is
depends on the type of user interface currently in use. For example, a graphical UI might want to be able to display which of the
internal steps are ready to start processing, because all the required input fields are populated with valid values. And it
might want to have a 'Start' button a user can click, which should kick off one or several processing steps.
Or, in the case of a commandline-interface, the application might want to check whether all required pipeline inputs are
set when a pipeline input was set, and once that is the case, it might want to process all pipeline steps, rejecting
every other input attempt.

To accomodate all such scenarios (or at least as many as poosible), *Kiara* has the concept of a pipeline 'controller'.
This is an object of a subclass of [PipelineController][kiara.pipeline.controller.PipelineController],
which can implement one or several callback methods, and implement the necessary reactive logic for a given frontend-scenario.

## Callback methods

So, in order to implement your own pipeline controller you have to implement one or several of the following methods in your subclass:

- [``pipeline_inputs_changed``][kiara.pipeline.controller.PipelineController.pipeline_inputs_changed]: if you want to react to user input
- [``pipeline_outputs_changed``][kiara.pipeline.controller.PipelineController.pipeline_outputs_changed]: if you want to react to a pipeline output being ready
- [``step_inputs_changed``][kiara.pipeline.controller.PipelineController.step_inputs_changed]: if you want to react to an input field change for one or several internal pipeline steps
- [``step_outputs_changed``][kiara.pipeline.controller.PipelineController.step_outputs_changed]: if you want to react to one or several step output changes (that happens basically whenever a step finished processing)

## Helper methods

The [PipelineController][kiara.pipeline.controller.PipelineController] base class also gives you several convenience methods
you can use to investigate the current state of a pipeline and its data fields, as well as kick off processing steps. The main ones are:

- [``process_step``][kiara.pipeline.controller.PipelineController.process_step]: start processing the step with the provided id, if the steps inputs are not ready yet an exception will be thrown
- [``step_is_ready``][kiara.pipeline.controller.PipelineController.step_is_ready]: check whether a specific step is ready for processing (aka all inputs are valid)
- [``step_is_finished``][kiara.pipeline.controller.PipelineController.step_is_finished]: check whether a specific step has been processed successfully, and none of the steps upstream this one depends on has reveived new inputs
- [``pipeline_is_ready``][kiara.pipeline.controller.PipelineController.step_is_ready]: check whether all pipeline inputs are set and valid, which means the whole pipeline can be processed, no further external input is necessary
- [``pipeline_is_finished``][kiara.pipeline.controller.PipelineController.step_is_finished]: check whether the pipeline has finshed processing successfully, and no pipeline input was set since that happened
- [``set_pipeline_inputs``][kiara.pipeline.controller.PipelineController.set_pipeline_inputs]: can be used to set inputs from within the controller (but those can alse be set through other means, for example through the workflow inputs value set).

## Example controller

Lets say we want to create a very simple controller that prints out every data field change, and lets users manually kick off
processing steps. The code for such a controller would look something like this:

``` python
{{ get_src_of_object('kiara.examples.example_controller.ExampleController') }}
```

Then, executing code like this:

``` python
{{ get_src_of_object('kiara.examples.example_controller.execute_pipeline_with_example_controller') }}
```

will yield output like:

```
Pipeline inputs changed: ['a']
Step inputs changed, new values:
  - step 'and':
      a: True
Pipeline inputs changed: ['b']
Step inputs changed, new values:
  - step 'and':
      b: False
Executing step: and
Step inputs changed, new values:
  - step 'not':
      a: False
Step outputs changed, new values:
  - step 'and':
      y: False
Executing step: not
Step outputs changed, new values:
  - step 'not':
      y: True
Pipeline outputs changed: ['y']
Pipeline result:
{'y': True}
```
