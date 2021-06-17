# Data storage

This is a document to describe my plans for storing data (and metadata) in *kiara*. (Almost) nothing I describe here is inmplemented yet, so it only reflects my current thinking. I think the overall strategy will hold, but there might be changes here and there.

## The problem

*kiara*s main functionality centers around transforming input data sets to output data sets. Those outputs need to be stored, to be of any use later on. Obviously. When deciding how to do this, we must take into account concerns about performance, disk- and memory-usage, data versioning, which metadata to attach, in what way, how to deal with metadata schemas (and versioning of both), etc.
