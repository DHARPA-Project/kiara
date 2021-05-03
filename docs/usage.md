# Usage


## Getting help

To get information for the `kiara` command, use the ``--help`` flag:

{{ cli("kiara", "--help") }}

## 'module'-related subcommands

{{ cli("kiara", "module", "--help") }}

### list all available modules

List all available modules ([core](/modules/core_modules)- as well as as [pipeline](/modules/pipeline_modules)-ones).

{{ cli("kiara", "module", "list") }}

### display details about a module type

Display information about a modules, like description, configuration schema, source code (in case of a core-module).

#### ...for a core module

{{ cli("kiara", "module", "describe-type", "and") }}

#### ...for a pipeline module

{{ cli("kiara", "module", "describe-type", "nand") }}

### get properties of an instantiated module

In this context, an instantiated module is a module that was created with some optional configuration. It is necessary to know whether/what configuration is used, because that can change characteristics like available input/output fields and their schemas, which is why this gets it's own command section.

!!! note
This command also can take module configuration, in different forms. This will be documented in the future.

#### ...for a core module

{{ cli("kiara", "module", "describe-instance", "and") }}

#### ...for a pipeline module

{{ cli("kiara", "module", "describe-instance", "nand") }}

## pipeline-specific sub-commands

This subcommand lets you display module details that are specific to pipeline-type modules.
To print graphs, currently Java (JRE) needs to be installed, as well as the ``asciinet`` python package from GitHub (``pip install 'git+https://github.com/cosminbasca/asciinet.git#egg=asciinet&subdirectory=pyasciinet``). This might change in the future.

### print the data flow graph for a pipeline

Use the ``--full`` flag to display the non-simplified graph.

```
> kiara pipeline data-flow-graph nand
...
...
```

### print the execution graph for a pipeline

```
> kiara pipeline execution-graph nand
...
...
```
