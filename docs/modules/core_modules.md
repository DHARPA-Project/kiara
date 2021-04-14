# *Core* modules

*Core modules* are implemented as Python classes which inherit from the abstract base class [KiaraModule][kiara.module.KiaraModule]. They need to implement 3 methods:

  - [``create_input_schema``][kiara.module.KiaraModule.create_input_schema]: returns a description of the input(s) this module takes
  - [``create_output_schema``][kiara.module.KiaraModule.create_output_schema]: returns a description of the output(s) this module produces
  - [``process``][kiara.module.KiaraModule.process]: the actual processing step, to transform the inputs into outputs

!!! note
    Ideally, a modules function is [idempotent](https://en.wikipedia.org/wiki/Idempotence), but it's allowed to have calls to functions that return
    random objects within, as long as it's ok for the resulting output to be cached/re-used.

An example of such a module would be the [AndModule][kiara.modules.logic_gates.AndModule], which is a simple module that computes the logic 'and' operation:

``` python
{{ get_src_of_object('kiara.modules.logic_gates.AndModule') }}
```
