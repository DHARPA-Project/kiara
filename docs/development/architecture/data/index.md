From looking at the user stories, and after listening to the interviews Lorella conducted and also considering my own
personal experience in eResearch, I think its save to say that the central topic we are dealing with is data. Without data,
none of the other topics (workflows, visualisation, metadata...) would even exist. Because of its central nature I want to lay out the different forms it comes in, and which characteristics of it are
important in our context.

## What's data?

Data is created from sources. Sources come in different forms (analog, digital) and can be anything from handwritten
documents in an archive to a twitter feed. Photos, cave-paintings, what have you. I'm not websters dictionary, but I think
one usable working definition of data could be a 'materialized source', in our context 'materialized source in digital form'.
From here on out I'll assume we are talking about 'digital' data when I mention data.

One thing I'll leave out in this discussion is what is usually called 'dirty data' in data engineering, although it is
an important topic. Most of the issues there map fairly well to the structured/unstructured thing below. There are a few
differences, but in the interest of clarity let's ignore those for now...

## Structured data / Data transformations

Important for us is that data can come in two different formats: unstructured, and, who'd have guessed... structured. The same
piece of data can be theoretically expressed in structured as well as unstructured form: the meaning to a researcher would
be 100% the same, but the ways to handle, digest and operate with the data can differ, and in most scenarios adding structure
opens up possibilities to work with the data that weren't there before. In my head I call those two forms 'useless', and
'useful' data, but researcher usually get a bit agitated when I do, so I have learned to not do that in public anymore.

For researchers, the most (and arguably only) important feature of 'structure' is that it enables them to
do *more* with the data they already possess. By means of computation. I think it's fair to say that only structured data
can be used in a meaningful way in a computational context. With the exception that unstructured data is useful input to
create structured data.

One more thing to mention is that the line between structured and un-structured is sometimes hard to draw,
and can depend entirely on context. "One persons structured data is another persons unstructured data.", something like that.
In addition, in some instances unstructured data can be converted to structured data trivially, meaning without much effort
or any user-interaction. I'd argue we can consider those sorts of datasets basically 'structured'.

### Example

Lets use a simple example to illustrate all that: *a digital image of a document*.

Depending on what you are interested in, such an image might already be structured data. For example it could contain geo-tags, and a
timestamp, which are both digitally readable. If you want to visualize on a map where a document is from, you can do that instantly.
Structured data, yay!

Similarly, if you are interested in the color of the paper of the document (ok, I'm stretching my argument here as this seems fairly
unlikely, but this is really just to illustrate...), you might get the color histogram of the image (which is trivial to extract,
but needs some batch-computation), and for your purposes you would also consider the image file structured data.

Now, if you are interested in the text content of the document, things get more interesting. You will have to jump
through some hoops, and feed the image file to an OCR pipeline that will spit out a text file for example. The data
itself would still be the same, but now computers can access not only some probably irrelevant metadata, but also the text content,
which, in almost all cases, is where the 'soul' of the data is.

It could be argued that 'just' a text file is not actually structured. I'd say that groups of ascii-characters that
can be found in english-language dictionaries, separated by whitespaces and new-lines can be considered a structure,
even if only barely. The new format certainly allows the researcher to interact with the data in other ways (e.g. full-text search).

We can go further, and might be interested in characteristics of the text content (language, topics, etc.). This is where
the actual magic happens, everything before that is just rote data preparation: turning unstructured (or 'other-ly' structured)
data into (meaningful) structured data... On a technical level, those two parts (preparation/computation) of a research workflow might look (or be)
the same, but I think there is a difference worth keeping in mind. If I don't forget I'll elaborate on that later.

## 'Big-ish' data

I'm not talking about real 'Big data'-big data here, just largish files, or lots of them, or both. I don't think we'll encounter many use-cases where we have to move
or analyze terabytes of data, but I wouldn't be surprised if we come across a few gigabytes worth of it every now and then.

There are a few things we have to be prepared for, in those cases:

- transferring that sort of data is not trivial (esp. from home internet connections with limited upload bandwidth) -- and we will most likely have to be able to offer some sort of resumable-upload (and download) option (in case of a hosted solution)
- if we offer a hosted service, we will have to take into account and plan for this, so we don't run out of storage space (we might have to impose quotas, for example)
- computation-wise, we need to make sure we are prepared for large datasets and handle that in a smart way (if we load a huge dataset into memory, it can crash the machine where that is done)
- similarly, when we feed large datasets into a pipeline, we might not be able to just duplicate and edit the dataset like we could do for small amounts of data (too expensive, storage-wise) -- so we might need to have different strategies in place on how to execute a workflow, depending on file sizes (for example some sort of copy-on-write)
