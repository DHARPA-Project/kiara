# *Pipeline* modules

*Pipeline modules* are assemblies of a number of other modules (which can be either type, *core*, or *pipeline*), including descriptions of how some modules inputs
are connected to other modules outputs. Module inputs that are not connected to one or several other modules outputs are expected to receive (external) user input.

## Example: the ``nand`` module

An example of a configuration for such a *pipeline module* would be the ``nand``-pipeline, which contains two *core modules* ([AndModule][kiara.modules.logic_gates.AndModule] and [NotModule][kiara.modules.logic_gates.NotModule]), where the latters only input is connected to the formers output), and which performs the, as you might have guessed, the [*nand*](https://en.wikipedia.org/wiki/NAND_logic) operation:

```yaml
{{ get_pipeline_config('nand') }}
```

## Configuration schema

In order to create the configuration that in turn will create a ``PipelineModule`` object, we need to follow a schema. The description of this schema can be found [here](../../development/entities/modules/#pipelinemoduleconfig) (json schema format) and [here][kiara.config.PipelineModuleConfig] (Python model class).
