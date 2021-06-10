# Getting started

This guide walks through some of the important (and some of the lesser important) features of *kiara*, the goal is to introduce
new users to the overall framework, so they can get a feeling for what it can do, and whether it might be useful for their
own usage scenarios.

As example data, we'll be using two csv files that were created by my colleague [Lena Jaskov](https://github.com/yaslena): [files](https://github.com/DHARPA-Project/kiara_modules.playground/tree/develop/examples/data/journals)

The files contain information about connection (edges) between medical journals (``JournalEdges1902.csv``), as well as additional metadata for the journals themselves (``JournalNodes1902.csv``). We'll use that data to create table and graph structures with *kiara*.

## Setting up kiara

For now, there are no published binary versions of kiara, so we'll install the Python package in a virtual environment.
While we're at it we'll check out the [kiara_modules.playgrounds repository](https://github.com/DHARPA-Project/kiara_modules.playground), because we can use that later to create our own *kiara* modules and pipelines:

```console
git clone https://github.com/DHARPA-Project/kiara_modules.playground.git
cd kiara_modules.playground
python3 -m venv .venv
source .venv/bin/activate
make init
```

This will take a while, because it sets up a full-blown development environment. Which is not really necessary for us
at this stage, but hey...

## Checking for available modules

First, let's have a look which modules are available, and what we can do with them:

{{ cli("kiara", "module", "list", max_height=320) }}

## Loading data into a table

Tables are arguably the most used (and useful) data structures in data science and data engineering. They come in different
forms, and some people call them spreadsheets, or dataframes. We won't do that, we'll call them tables. And when we talk
about tables, we specifically talk about [Apache Arrow Tables](https://arrow.apache.org/docs/cpp/tables.html#tables),
because *kiara* really likes the [Apache Arrow project](https://arrow.apache.org/docs/index.html), and thinks that
there is a very high probability that it will become a de-facto standard in this space (if it isn't already).
Why Arrow tables are better than others is a topic for another time, plus, in practical terms the underlying implementation
of the data structures that are used by *kiara* will be transparent to most users anyway.

A depressingly large amount of data comes in csv files, which is why we'll use one as an example here. Specifically, we will
use [``JournalNodes1902.csv``](https://github.com/DHARPA-Project/kiara_modules.playground/blob/develop/examples/data/journals/JournalNodes1902.csv). This file contains information about historical medical
journals (name, type, where it was from, etc.). We want to convert this file into a 'proper' table structure, because
that will make subsequent processing faster, and also simpler in a lot of cases.

So, after looking at the ``kiara module list`` output, it looks like the ``table.from_csv`` module might be a good fit for us. *kiara* has the [``run``](../running_modules) sub-command, which is used to execute modules. If we
only provide a module name, and not any input, this command will tell us what it expects:

{{ cli("kiara", "run", "table.from_csv") }}

As makes obvious sense, we need to provide a ``path`` input, of type ``string``. The *kiara* commandline interface can
take complex inputs like dicts, but fortunately this is not necessary here. If you ever come into a situation where you need this, check out [this section](../..//usage/#complex-inputs).

For simple inputs like strings, all we need to do is provide the input name, followed by '=' and the value itself:

{{ cli("kiara", "run", "table.from_csv", "path=examples/data/journals/JournalNodes1902.csv", max_height=340) }}

Although you can't see it from the output, *kiara* actually created an Arrow Table object from the csv. After *kiara* finished, this output was lost, though, since we didn't do anything with it. In most cases, we'll want to save this object. Which we can do with the ``run`` command: we just need to set the ``--save`` flag.

This will prompt *kiara* to save the output of the workflow we are running into the internal *kiara* data store, along
with the values metadata and a few other bits and pieces. So, let's run that command from before again, but this time
with the ``--save`` option. We'll also use ``--id getting_started_example --output format=silent``, because we want
to give our saved data a meaningful alias, and we are not interested to see the table content on the terminal (again):

{{ cli("kiara", "run", "--id", "getting_started_example", "--output", "format=silent", "--save", "table.from_csv", "path=examples/data/journals/JournalNodes1902.csv") }}

To check whether that worked, we can list all of our items in the data store, and see if the one we just created is in there:

{{ cli("kiara", "data", "list") }}

And, yay, it is! It is named ``getting_started_example.table`` after the workflow id, and the name of the output field of the workflow.

We can also look at the metadata *kiara* stored for this specific item:

{{ cli("kiara", "data", "explain", "getting_started_example.table", max_height=320) }}

This metadata is useful internally, because it enables *kiara* to be very selective about which parts of a dataset
it actually loads into memory, if any.

One thing that is noteworthy here is the ``load_config`` section in the metadata. When saving the value, *kiara* automatically
generated this configuration, and it can be used later to load and use the exact same table file, in another workflow.

## Querying the table data

This section is a bit more advanced, so you can skip it if you want. It's just to show an example of what can be done with
a stored table data item.

We'll be using the [graphql](https://graphql.org/) query language to find the first 10 german-language journals, along with their city it is from. The query for this looks as follows:

{{ inline_file_as_codeblock('examples/data/journals/query.graphql') }}

And the *kiara* module we are going to use is called ``table.query.graphql``. Let's check again the parameters this module expects:

{{ cli("kiara", "run", "table.query.graphql") }}

Aha. ``table``, and ``query``. Good, we have both. In this example we'll use the data item we've stored as input for another workflow. That goes like this:

```
➜ kiara run table.query.graphql table=value:getting_started_example.table query="$(cat examples/data/journals/query.graphql)"

Output data

  ╭─ field: query_result ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
  │ {'df': {'row': [{'City': 'Berlin', 'Label': 'Die Krankenpflege'},                                                                                                │
  │                 {'City': 'Berlin',                                                                                                                               │
  │                  'Label': 'Die deutsche Klinik am Eingange des zwanzigsten '                                                                                     │
  │                           'Jahrhunderts'},                                                                                                                       │
  │                 {'City': 'Berlin', 'Label': 'Therapeutische Monatshefte'},                                                                                       │
  │                 {'City': 'Berlin',                                                                                                                               │
  │                  'Label': 'Allgemeine Zeitschrift für Psychiatrie'},                                                                                             │
  │                 {'City': 'Berlin',                                                                                                                               │
  │                  'Label': 'Archiv für Psychiatrie und Nervenkrankheiten'},                                                                                       │
  │                 {'City': 'Berlin', 'Label': 'Berliner klinische Wochenschrift'},                                                                                 │
  │                 {'City': 'Berlin', 'Label': 'Charité Annalen'},                                                                                                  │
  │                 {'City': 'Berlin',                                                                                                                               │
  │                  'Label': 'Monatsschrift für Psychiatrie und Neurologie'},                                                                                       │
  │                 {'City': 'Berlin', 'Label': 'Virchows Archiv'},                                                                                                  │
  │                 {'City': 'Berlin',                                                                                                                               │
  │                  'Label': 'Zeitschrift für pädagogische Psychologie und '                                                                                        │
  │                           'Pathologie'}]}}                                                                                                                       │
  ╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

The ``$(cat query.graphql)``-thing is really just a convenient way to not have to type the query string by hand, just ignore it...

## Generating a network graph

Since what we actually want to do is generating a network graph from our two csv files, we'll have a look at the list of
modules again, and it looks like the ``network.graph.from_csv`` one might do what we need.

But we are not sure. Luckily, *kiara* has some ways to give us more information about a module.

The first one is the ``module explain-type`` command:

{{ cli("kiara", "module", "explain-type", "network.graph.from_csv", max_height=320) }}

Uh. That's a handful. To be honest, that's mostly useful for when you want to start creating modules or pipelines
for *kiara* yourself. The ``module explain-instance`` command is more helpful, though:

{{ cli("kiara", "module", "explain-instance", "network.graph.from_csv", max_height=320) }}

The 'inputs' section is most interesting, it's basically the same information we get from running ``kiara run`` without any inputs. Using the information from that output, and after looking at the headers of our csv files, we can figure out how to assemble our command:

{{ cli("kiara", "run", "network.graph.from_csv", "edges_path=examples/data/journals/JournalEdges1902.csv", "source_column=Source", "target_column=Target", "nodes_path=examples/data/journals/JournalNodes1902.csv", "nodes_table_index=Id", "--save", "--id", "generate_graph_from_csvs") }}

!!! note
    Yes, we could use the nodes table we loaded earlier here. But we don't. For reasons that have nothing to do with what makes sense here.

To confirm our graph is stored, let's check the data store:

{{ cli("kiara", "data", "explain", "generate_graph_from_csvs.graph") }}

## Investigating the graph

Now we might want to have a look at some of the intrinsic properties of our graph. For that, we will use the ``network.graph.properties`` module:

{{ cli("kiara", "run", "network.graph.properties", "graph=value:generate_graph_from_csvs.graph" "--save) }}

## Finding the shortest path

Another thing we can do is finding the shortest path between two nodes:

{{ cli("kiara", "run", "network.graph.find_shortest_path", "graph=value:generate_graph_from_csvs.graph", "source_node=1", "target_node=2") }}

That's that, for now. This is just a first draft, let me know all the things I should change, explain better, etc.
