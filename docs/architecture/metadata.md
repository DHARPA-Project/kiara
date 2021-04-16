## Metadata

Metadata is more important in research than in other fields. Metadata can be used to, among other things, track provenance of data,
describe authorship, time of creation, location of creation, describing the 'shape' of data (schemas, etc.).

In some cases it's not easy to determine what's data and what's metadata. Sometimes metadata becomes data ("One persons metadata...").
Handling metadata is difficult, and it regularly gets lost somewhere in the process. Creating metadata in the first place can be
very time-consuming, I would wager that is more true in the digital humanities than in the harder sciences.

With the growing popularity of the open data movement, people are getting more aware of the importance of metadata, and
there is a growing infrastructure and services around all of this (DOIs, RDF, 'linked data', Dublin core, ...). None
of it is easy or intuitive to use, but I guess that's just the nature of the beast.

I think it is safe to say that whatever we come up with has to be able to create and handle metadata in some way or form,
and personally, I think we should 'bake' metadata handling in from the beginning. Looking at the user-stories it's quite
clear that this an important topic. How exactly that will look, I think there is some leeway, but all architecture proposals
should at least include some indication on how this would be handled.

### Schema information

One important piece of metadata is often schema information: what exactly is the shape of the data, how can I read it?
In some cases this can be inferred from the data easily, sometimes it's even obvious. But often that is not the case at all,
which makes things like creating generic data exploration tools very hard, if not impossible.
We would have, if we choose to create and attach it, all that information available, always, which would mean it would be easy
to create generic, peripheral tools like a generic data explorer. It will, of course, also make it easier to re-use such data in other workflows,
because users would not have to explicitly specify what their data is; we could infer that from the attached schema.

### Workflow metadata

One thing that is specific to our application is that we have full control over every part of the data-flow. So, we can
attach metadata of all inputs and previous steps to each result (or intermediate result) along the way. Which is quite an
unique opportunity; this is often not available at all, or has to be done manually by the researcher.

There is a lot that can be done with such annotated (result-)data. For example, each data set can include pointers to all
the original data that was involved in creating it (or it could even include that data itself), as well as a description
of all the transformation steps it went through. This means that one could potentially create a full visual representation of
what happened to the data since it was created, just by looking at the attached metadata. This is usually impossible, because
there is never a sort of 'unbroken cold-chain' of metadata available. Of course, this would also help with reproducability and
related issues.

This possibility is something I'm particularly excited about, even though it does not directly appear in any of our user
stories (so would not be a core requirement). But it's one of the things I would have liked to have available often in the past.
