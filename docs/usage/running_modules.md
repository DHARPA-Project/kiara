# Running workflows

The *kiara* cli includes a command to execute a workflow via the terminal:

{{ cli("kiara", "run", "--help") }}

### Options

The most important parameters are the ``MODULE`` and ``INPUTS`` arguments, which will be explained below. Apart from those,
the command let's you costumize a few things:

``--id``
:    The id of the workflow, this affects the auto-generated alias(es) for output values if ``--save`` is used

``--module_config``
:    This option allows to provide configuration for a module. In most cases this won't be necessary, so we won't go into
     it here. The format of the configuration is explained [here](../../usage/#complex-inputs).

``--explain``
:    If this flag is set, *kiara* will print out information about the state of the workflow and its inputs/outputs.

``--output``
:    Let's you tweak the output of this command. In most cases, this won't be needed. Documentation for this will be added later.

``--save``
:    If set, the outputs of this workflow will be saved into the *kiara* data store. You can get see the stored items
     via ``kiara data list`` and the [other data-related subcommands](./data.md)


### Arguments: *module* and *input*

Obviously, we need to specify the module we want to run (list available ones via ``kiara module list``). If we don't
specify any inputs (and the module in question doesn't have defaults for one or several inputs), *kiara* will tell us
what we need:

{{ cli("kiara", "run", "logic.xor") }}

Here we are tols we need to inputs: ``a`` and ``b``, both booleans. How to provide inputs for a *kiara* run command can
be a bit tricky if more complex input data types are required (e.g dicts). How to do this is explained [here](../..//usage/#complex-inputs).

For now, we don't need to worry about it because we only need booleans, which can be provided as ``true`` and ``false`` strings:

{{ cli("kiara", "run", "logic.xor", "a=true", "b=true") }}

In case we are interested in more than just the output field, we can re-run this command with the ``--explain`` flag:

{{ cli("kiara", "run", "--explain", "logic.xor", "a=true", "b=true", max_height=320) }}

### Saving outputs

It's very early days in the implementation of this feature, and it only works for the ``table`` and ``network_graph`` data
types, but basically, all you need to do is add the ``-save`` flag, and *kiara* will store the result data as items in the
*kiara* data store (in the format: ``[workflow_id].[output_name]``)

Let's say we want to create an (Apache arrow) table object out of a csv file, and store it in the data store, we could use
the ``tabular.import_table_from_file`` module:

{{ cli('kiara', 'run', '--output', 'format=silent', '--save', 'tabular.import_table_from_file', 'path=docs/example_data/JournalNodes1902.csv') }}

Now we can check that our table is present in our data store:

{{ cli('kiara', 'data', 'list', '--ids') }}
