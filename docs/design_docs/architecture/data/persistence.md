# Data persistence

This is a document to describe my plans for storing data (and metadata) in *kiara*. (Almost) nothing I describe here is inmplemented yet, so it only reflects my current thinking. I think the overall strategy will hold, but there might be changes here and there.

## The problem

*kiara*s main functionality centers around transforming input data sets to output data sets. Those outputs need to be stored, to be of any use later on. Obviously. When deciding how to do this, we must take into account concerns about performance, disk- and memory-usage, data versioning, which metadata to attach, in what way, how to deal with metadata schemas (and versioning of both), etc.

## The solution

Well, solution. This is my current thinking of how to tackle the problem in a way that takes into account all of the aspects described above, while still being flexible enough to hopefully be able to incorporate solutions for future unforsseen issues.

I am having trouble coming up with a good structure for this document, so I think I'll just try to tell the story from the point of view of data. Starting from a state where data exists outside of *kiara*, to when it is in a state to be ready to be published. As with everything I'm writing here as an explanation of generic and abstract concepts, some of the technical details I'm describing might be simplified to the point of being incorrect...

## The 7 stages of data

One thing I'd like to say before I start to describe those stages: the transformation of a dataset, from one stage to the next, **always** **always** **always** happens by piping the dataset through a *kiara* module. At **absolutely** **no** point is this done without *kiara*s involvement and knowledge. The dataset is used as input for a module, and the result (technically a new dataset) is a representation of the dataset in its next stage. This is important to keep in mind, as it is crucial for us so we can track data lineage. I'll write more on the specifics of this below, where it makes more sense.

### 1) Unmanaged

At the beginning, there was csv. Whether I like it or not, csv is the most predominant form data comes in. Csv is bad in a lot of ways, but in my mind the worst thing about it is that it is schema-less. True, in some cases you have a header-line, which gives you column-names, but that's not a requirement. Also, in a lot of cases you can auto-determine the type of each column, and luckily libraries like Pandas or Apache Arrow solved that problem for us so we don't have to do it ourselves every time. But those auto-parsers are not fool-proof, and you end up with integers where you wanted floats (or doubles), or integers where you wanted strings, or vice versa.

In some cases we get data in a form that includes at least a semi-schema. Like a sqlite database file (which is more 'strongly' typed). But it's a lucky day when we get data that contains metadata about authorship, how and when it was created, from what sources, etc.

### 2) Onboarded

This is the first thing we need to do to unmanaged data: we need to 'onboard' it, so *kiara* knows the data exists, and what exact bytes it consists of. This last thing is very important: we have to be able to make sure the data we are talking about is not being changed externally, a lot of things in *kiara*s approach to data depend on this.

Practically, in most cases this means *kiara* will copy one or several files into a protected area that no other application can/should access. That way we always have a reference version of the dataset (the bytes) we are talking about.

One thing *kiara* does at this stage is give the dataset a uniuqe id, which can be used to reference it later (by users, or other objects/functions). Another thing is to collect some basic metadata: when the file/folder was imported, from what original path, what the filenames are, mime-type, size of files, original file attributes (creation data, permissions, etc.). This can all be captured automatically. We can also record who it was that imported the dataset, if we have some app-global configuration about the current user, like a full name and email-address. Note, that this might or might not be the person who created the dataset.

So, at this stage all we did was copy a file(set) into a protected area to sort of 'freeze' it, and augment it with very basic metadata. We don't know anything about the nature of the dataset yet, all we know is the bytes the datasets consists of. It is important to point out that we would not have to store those chunks of bytes as files again, using the same structure as the original set of files. The dataset ceased to be 'files' here for us, we are only interested in the chunks of bytes (and their meaning) from here on out. We could store the data in an object store, zipped, tarred and feathered (pun intended). Or as byte-stream directly on block storage, if we were crazy enough.

A side-note that makes things a bit more complicated, but it is probably necessary to address potential concerns: no, we don't actually need to copy the files, and can leave them in place and only generate the metadata and id for them. This might be necessary in cases where the source data is very big (photos, movies, audio-files, other large datasets). I don't think we need to figure out how exactly we deal with this scenario right now, but it basically comes down to making the user aware of what is happening, and what the implications are if the source data is changed externally (inconsistent metadata and potential incorrect result data-sets further down the line). There are strategies to help prevent some of those potential issues (checksums, for example), but overall we have to acknowledge that working with large-sized datasets is always a challenge, and in some cases we might just have to say: "sorry, this is too big for us right now".

### 3) Augmented with more (basic) metadata

To recapitulate: at this stage we have data (chunks of bytes -- not files!!! hit yourself over the head twice with something semi-heavy if you are still think in terms of files from here on out!) in a protected area, some very basic metadata, and an id for each dataset. We might or might not have authorship metadata (arguably one of the most important pieces of metadata), depending on whether who 'onboarded' the dataset actually created it.

So, as a first step and following good practice, at this stage we should try to get the user to tell us about authorship and other core metadata about our dataset (licensing, copyright, ...). I don't think we can make this step mandatory, in practice, but we should push fairly hard, even if that means a slight decrease in user experience. It is very important information to have...

So, one thing we could do was to have a checkbox that lets the user confirm: I created the data (in which case we can just copy the 'imported-by' field).



### 3) Typed

Chunks of bytes are not very useful by itself. They need to be interpreted to be of use. This means: determining in some way what the structure of the chunks of bytes is, and then applying common conventions for that particular structure (aka data type/format) when reading the chunks of bytes. Therefore 'interpreting' the chunks of bytes. This is a very obvious thing that happens all the time we use computers, but I think it makes sense to point it out here, because usually this is transparent to the user when they click an 'Open file' button in an application, and even some developers are ignorant to the underlying concept (and can afford to be, since they usually work several abstraction layers above where that is happening).

To encapsulate this concept, we will create a 'data type' for each important group of datasets that share some important characteristics. Examples for very simple data types are strings, integers, booleans. I'll ignore those, because those are trivial to use, and that triviality actually makes it harder to explain the concept I'm talking about. More relevant data types are: 'table', 'network graph', 'text corpus', 'photo collection'. Every data type inherently contains a description of, well, the 'type' of data represented by it, and, with that, information about how a user or code caqn access the actual data, and/or some of its properties.

From here on out, I'll focus on tabular data in some form or other, since I expect that this will be one of our most important (base-) data types. I expect the reader to 'translate' whatever I'm saying below to other types, and extrapolate the practical differences.

So, to explain this step I decided to look at three different use-cases (mostly because we use them in 2 of our example workflows, so people should be familiar with them):

- a csv file with tabular data
- an imported folder of text files (a corpus)
- two imported csv files containing edge and node information to form a network graph

#### Example: tabular data

This is the simplest case, and very common: we have a csv file, and need to have some sort of tabular data structure that we can use to query and analyze the data contained in it.

Let's assume we have onboarded a csv file using *kiara*, so we have a dataset id that we use to point to it. Technically, this dataset is already 'typed': it has the type 'file'. This is not a very useful type, all it allows can tell us is a file name (which in a way is metadata), and the file content. We can ask *kiara* to interpret the content of this file as table, though, because we know it must be one. This means we 'overlay' a different, more specific data type on top of the same data.

Under the hood, *kiara* will use the Apache Arrow [``read_csv``](TODO) helper method, which is very smart and fast, and it can create an Arrow Table object out of a csv file. It can figure out file encoding, column names (if present), column types, seperator characters. This detection is not fool-proof, but should work good enough in practice that we don't need to worry about it here. What really happens here is that the ``read_csv`` method is not just reading our data, but also, at the same time, is adding some important metadata to our dataset. Pandas can do the same with its csv import method. Even though this adding of metadata is more or less transparent to the user -- so they are not really aware of it -- it happens, and it is a very important thing that must happen to make our dataset useful. In our application, we might or might not want to ask users whether the extracted column names and types are correct, but this is a UI-specific implementation detail.

So, considering all this, the important point here is that at this stage we have actual 'table' data, there is no need for the original csv file anymore (except as a reference for data lineage purposes). Our dataset is now of the data type 'table'. Which means we have an assurance that we can query it for table metadata properties (number of rows, number and name of columns, column types, size in bytes, etc.). And we can apply functions against it that are commonly applied against tabular data (sql queries, filters, etc.). That means, a 'data type' is really just a convention, a marker, that tells us how the bytes are organized for a particular set of bytes, and how to interact with it. This is all users and 3rd party-code needs to worry about. Implementation details about how this data is stored or loaded are irrelevant on this level of abstraction. This reduces complexity for *kiara*s external facing API, while, of course, introducing some extra complexity internally.


#### Example: text corpus

The source data comes as a folder of files, each file contains (just) text (not structured like json, csv, etc.). When we do the 'onboarding' step for this data, all we do is copy the files verbatim into a new location. There could be some metadata implicit in the relative paths of each file (e.g. languages -- files for the same language live in a subfolder named after the language), and there can also be some metadata in the file names. We preserve all that metadata by copying the folder one-to-one, without changing anything. But it is important to note that this metadata, as of yet, is still uncaptured.

The 'soul' of this dataset (meaning: the properties of the dataset we are interested in and we want to use in our investigation, and which will hopefully answer our research question) is in the content of each text file (aka the unicode encoded chunks of bytes). It is important to say again: at this stage the dataset ceased to be a set of files! It is a dataset within *kiara* that has an id (a single one! not one for every text!), and it has a basic set of metadata fields (the ones we could collect automatically). Yes, the dataset is backed by a set of files in the *kiara* data store, but that is an implementation detail nobody needs to know about, and I think we should try hard to hide from users. If you haven't noticed so far: I strongly believe the file metaphor is a distraction, and not necessary for us, except when import/export is concerned.

Anyway, *kiara* does not know much about the dataset at this stage. To be of actual use, we need to interpret the data. In this case, we know we want to interpret the data as a text corpus.

The most basic shape we can imagine a text corpus to look like is a list of strings (an array, or a single-column table). For making it easier to work with the text corpus in the future, let's make up a convention to save in tabular form, and the column containing the text items is always named ``text_content``. If we use, for example Apache Arrow to store that table, it makes the stored chunks of data much smaller (in comparison to text files), and it also makes the whole thing easier (at least faster) to query. It also allows us to easily attach more (meta-)data to the dataset.

!!! Note
    The distinction between data and metadata becomes a bit blurry here. In a lot of cases, when I say metadata, it is metadata from the point of view of the research proess, not metadata for *kiara*. I don't know how to make it clear which I'm talking about in each case without making this whole thing even more unreadable as it already is, so I will just have to ask you to figure it out yourself, in each case :-)

Because we didn't lose any of the implied metadata when onboarding our folder of text files, it would be a shame if we wouldn't actually capture it. In this case, let's assume we didn't have any subfolders (so no metadata in their name), but our files are named in a special way:

```
[publication_id]_[date_of_publishing]_[other_stuff_we_are_not_interested_in]
```

!!! Note
     The information about the format is important (in a way it is also an input) and we need to retrieve it somehow. This is a real problem that doesn't look like a big problem. But it is, for us. I'll ignore this here, because it would complicate things too much and is only of tangential relevance.

This means, we can extract the publication id and the date of publishing with a simple regular expression, and we can add a column for each one to our table that so far only contains the actual text for each item. The publication id will be of type string (even though some of the ids might be integers -- we don't care), and the publication date will be a time format. Now we have a table with 3 columns, and we can already filter the texts by date easily, which is pretty useful! We wouldn't, strictly speaking those two additional columns to have a dataset of type 'text corpus' but it's much more useful that way. As a general rule: if we have metadata like that, it should be extracted and attached to the data in this stage. It's cheap to do in a lot of cases, and we never know when it will be needed later.

What we have at this stage is data that has the attributes of a table (columns with name and type info, as well as rows representing an item and it's metadata). This is basically the definition of our 'text corpus' data type: something that allows us to access text content items (the actual important data) using a mandatory column named ``text_content``, and that has zero to N metadata properties for each of those text items. In addition, we can access other metadata that is inherited from the base type (table): number of rows, size in bytes, etc, as well as its lineage (via a reference to the original onboarded dataset).
Internally, we'll store this data type as an Arrow table, but again, this is an implementation detail, and neither user nor frontend needs to know about this (exceptions apply, of course, but lets not get bogged down by those just now -- none of them are deal-breakers, as far as I can see).

#### Example: network graph data

Similar to the text corpus case above, let's think about what a basic definition of a network graph data type would look like. It would have to include a list of nodes, and a list of edges (that tell us how those nodes are connected). Actually, the list of nodes is implied in the list of edges, so we don't need to provide that if we don't feel like it (although, that doesn't apply if we have nodes that are not part of any edge). In addition, both nodes and edges can have attributes, but those are optional. So, our network graph data type would, at a minimum, need to be able to give us this information about all this via its interface. [networkx](TODO) is one of the most used Python libaries in this space, so let's decide that internally, for us, a network graph is represented as an object of the  [Graph](TODO) class, or one of its subclasses.

This class will give us a lot of useful methods and properties to access and query, one problem left is: how do we create an object of this class in a way that fits with our overall strategy? We can't save and load a networkx object directly ([pickling](TODO) would be a bad idea for several reasons), so we need to create (and re-create) it via some other way.

For this, lets look at the constructor arguments of this class, as well as what sort of data types we have available that we can use to feed those arguments. [One option](TODO) apparently is to use a list of edges contained in a Pandas dataframe as input, along with a name of columns representing the names of source and target column name, something like:

```
        graph: nx.DiGraph = nx.from_pandas_edgelist(
            pandas_table,
            source_column,
            target_column,
        )
```

This could work for us: as in the other example, we can use a table as the 'backing' data type for our graph object. Considering a graph without any node attributes, we can have a table with a minimum of two columns, and via a convention that we just made up, we say that the source column should be called ``edge_source``, and the target column ```edge_target```. We wrap all this in an Arrow table again, and save it as such. And later load it again, assuming the same convention (which, basically, saves us from asking for 2 column names every time). If our graph also includes node attributes, all we do is extend the implementation of our network graph data type to create a second table with a required column ``node_id``, and one or several more columns that hold node attributes, similar to the metadata in our 'text corpus' example from above.


### 4) Transformed

With all that out of the way, we can finally do something interesting with the data. Everything up to this point was more or less housekeeping: importing, tagging, marking, organizing datasets. We still are operating on the same actual data as was contained in the original files (whatever type they were). But we now know exactly what we can do with it without having to ask questions.

Using the 3 example from above, we now know we have 3 datasets: one table, one text corpus (which is also a table, but a more specific one), and a network graph. And each of those datasets also comes with metadata, and we know what metadata files are available for what data types, and what the metadata means in each context.

A first thing we can do is automatically matching datasets to available workflows: we know what input types a workflow takes (that is included in each workflow metadata). So all we need to do is check the input types of each available workflow against the type of a dataset. This works even with different specificity: give me all workflows that take as input a generic graph. Or: give me all workflows that take a directed graph as input (this is information that is included in the metadata of each network graph dataset).
