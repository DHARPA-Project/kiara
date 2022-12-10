If we accept the premise that in the computational context we are really only interested in structured data, it follows
that there must be also 'things' that do stuff to our structured data. Let's call those things 'workflows'.

## Definition

I will concede that 'doing stuff to data' although entirely accurate is probably not the most useful of definitions.
Not Websters, all right? Well, how about:

    "A workflow is a tool to transform data into more structured data."

"more" can be read in one or all of those ways:

 - 'more data' -- we'll create what can be considered 'new' data out of the existing set
 - 'better structured' -- improve (and replace) the current structure (fix errors, etc.)
 - 'more structure' -- augment existing data with additional structure

In our context, workflows can also have secondary outcomes:

 - present data in different, more intuitive ways (e.g. visualisations), which researchers can use to get different ideas about the data, or new research questions
 - convert structured data into equivalent structured data, just a different format (e.g csv to Excel spreadsheet)
 - ... (I'm sure there's more, just can't think of anything important right now)

## Deconstructing a workflow

I've written more about it [here](https://github.com/DHARPA-Project/architecture-documents/blob/master/workflow-modularity/workflow-modularity.ipynb), but
conceptually, every data workflow is a collection of interconnected modules, where outputs of some modules are connected
to inputs of some other modules. The resulting network graph of modules defines a workflow. That's even the case for Jupyter
notebooks (which are really just fancy Python/R/Julia scripts); if you squint a bit you can see it:
the modules are functions that you call with some inputs, and you use the outputs of those functions (stored in variables) as inputs to
other functions. Move along, nothing to see here: this is really just how (most) programs work.

As I see it, there are three main differences to programs that are written in 'normal' computer engineering:

- the complexity of the resulting interconnected 'network graph' (the interconnection of functions) is usually lower
- it's a tad easier (or at least possible) to define, separate and re-use the building blocks needed in a majority of workflows (an example would be Numpy or the Pandas libraries, which are basically implementations of abstract problems that crop up often in this domain)
- it is possible to create workflows entirely out of modules that were previously created, with no or almost no other customization (normally, that customization would be very prominent in a program) -- often-times only some 'glue' code is needed

This means that data engineering workflows could be considered relatively simple script-like applications, where advanced
concepts like Object-Oriented-Design, Encapsulation, DRY, YAGNI, ... are not necessary or relevant (in most cases they wouldn't
hurt though).

## Data engineering

This way of looking at workflows is nothing new, there are quite a few tools and projects in the data engineering space
which deal with workflows in one level of abstraction or another.

As I'll point out below, the main difference to what we try to implement is that we'll add an element of 'interactivity'.
But I believe we can still learn a whole lot by looking at some aspects of those other tools.
I encourage everyone remotely interested to look up some of those projects, and maybe not read the whole documentation,
but at least the 'Why-we-created-yet-another-data-orchestrator', 'Why-we-are-better-than-comparable-projects' as well as
'What-we-learned'-type documentation pages you come across. 'I-tried-project-XXX-and-it-is-crap'-blog posts
as well as hackernews comment-threads related to those projects are usually also interesting. The '/r/dataengineering' and
'/r/datascience' sub-reddits are ok. But they are on Reddit, so, signal-to-noise is a bit, well..

Among others, interesting projects include:

- [dagster](https://github.com/dagster-io/dagster)
- [prefect](https://www.prefect.io/)
- [airflow](https://airflow.apache.org/)
- [luigi](https://github.com/spotify/luigi)

- also relevant, but less data-engineering-y: Node-RED, Apache NiFi, IFTTT, Zapier, Huginn, ...

## The 'workflow lifecycle'

One thing that I think is also worth considering is the different stages in the lifecycle of a workflow. For illustration,
I'll describe how each of those stages relates to the way data science is currently done with Jupyter, which is probably the most used tool
in this space at the moment.

### Workflow creation

This is the act of designing and implementing a new workflow transformed into one or a set of defined outcomes (which can be new data, or just a visualization, doesn't matter).
The actual creation of the workflow is similar to developing a script or application, and offers some freedom on how to implement it (e.g. which supporting
libraries to choose, whether and which defaults to set, ...).

In the Jupyter-case, this would be the iterative development of a Jupyter notebook, with one cell added after the other. One thing that is different for us
is that we will have a much stricter definition of the desired outcome of our workflow, whereas the creation of a Jupyter notebook is typically way more open-ended,
and a researcher would easily be able to 'follow some leads' they come across while working on the whole thing. This is a very important distinction that pays to
keep in mind, and I can't emphasize this enough: the workflows we are dealing with are a lot more 'static' than typical Jupyter notebooks, because we have decided in
advance which ones to implement, and how to implement them. There is not much we can do about this, and it's a trade-off with very little room to negotiate. This
has a few important implications on how our product is different from how data science is done by Jupyter users currently. I will probably mention this again
and again, because it is not intuitive at first, but has a big impact on how we view what we are building!

As per our core assumptions, end-users won't create new workflows, this is done by a group with a yet-to-be-determined 'special' skill set.

### Workflow execution

This is when a 'finished' workflow gets run, with a set of inputs that are chosen by the user. The schema/type of those inputs is a requirement
that has to be considered by the user. It's possible that a workflow allows for inputs to be in multiple formats, to make the users life easier (e.g. allow both '.csv' as well as '.json' formats),
but that also has to be documented and communicated to users. It is not possible to add elements to a workflow, and make it do different things
than it was designed to do. Our workflows are static, they never change (except in an 'iterative-development' sense where we release new versions)!

Compare that to a researcher who created their own Jupyter notebook: they will have run the workflow itself countless times by then, while developing it.
The execution phase is really only that last run that achieves the desired outcome, and which will 'fill' the notebook output cells with
the final results. That notebook state is likely to be attached to a publication. Often the data is 'hardcoded' into the notebook itself (for example
by adding the data itself in the git repo, and using a relative path to point to it in a notebook).
It is also possible, although not as common (as far as I've seen -- I might be wrong here) that researchers spend a bit more time on the notebook and
make the inputs easier to change, in order to be able to execute it with different parameters, quickly. This is more like what we will end up with,
although I'd argue that the underlying workflow is still much easier to change, fix, and adapt than will be the case with our application.

One difference between workflow creation and execution is that the creation part is more common for 'data scientists', and the execution part is a bigger
concern for 'data engineers' (both do both, of course). I think, our specific problem sits more in the data engineering than data science space (because
our main products are 'fixed'/'static' workflows), which is why I tend to look more for the tools used in that domain (data orchestrators, ...) than in the other
(Jupyter, ..) when I look for guidance.


### Workflow publication

Once a workflow is run with a set of inputs that yield a meaningful outcome for a researcher, it can be attached to a publication in some way.
This has one main purpose: to document and explain the research methodologies that were used, on a different level than 'just' plain language.

There is a long-term, idealistic goal of being able to replicate results, but the general sentiment is that it is unrealistic to attempt that at
this stage. It doesn't hurt to consider it a sort of 'guiding light', though.

It is getting more and more common for researchers to attach Jupyter notebooks to a publication. Jupyter notebooks are a decent fit for this
purpose, because the contain plain-text documentation, the actual code, as well as the output(s) of the code in a single file, that has a
predictable, well specified format (json, along with a required document schema). As our colleagues at the DHJ project have discovered, it's
not a perfect fit, but it can be bent to serve as the basis for a formal, digital publication.

In our case, it is my understanding that we would like to have an artefact like this too, and even though it's not one of the 'core' requirements,
it would be a very nice thing to have. One strong option is for us to re-use Jupyter notebooks for that. Depending on how we implement our
solution, we might already have one as our core element that 'holds' a workflow, in which case this is a non-issue.
Or, if that is not the case, we could 'render' a notebook from the metadata we have available, which should also not be too hard to do since the target
(the notebook) is well spec'ed. If that's the case, there is one thing I'd like to investigate before we commit though: what characteristics exactly are the
ones that make notebooks a good choice for that, and which one are detrimental? As I've mentioned, the DHJ project uses notebooks as the base
for creating article-(web)pages, and they came across some issues along the way. So I wonder: is there a better way to achieve the 'document and
explain research methodologies' goal than by using a Jupyter notebook? How would that look in a perfect world? How much effort would be involved?


## Interactivity / Long(-ish) running computations

One component that is different in our scenario to other implementations is the requirement for interactivity. In data-engineering,
this is never an issue, you describe your pipeline, then you or someone else uses that with a set of inputs, and off it goes,
without any further interaction. *Plomp*, notification, results, rinse, repeat.

For us that will be different, because we are creating a graphical user interface that reflects the workflow, and its state.
By definition, graphical user interfaces are interactive, and when a user triggers an action, they expect that to kick off
some instant response in the UI (maybe the change in a visualization, or a progress indicator, whatever).

### Computationally trivial/non-trivial

One main difficulty will be to find a good visual way to express what is happening to the user, ideally in the same way
for 2 different scenarios:

- computations that are computationally trivial, and will return a result back in a few seconds at most
- computations that take longer

In our workflows, I can see a few different ways those interactions can play out, depending on the characteristics of any particular workflow.

So, in the case where a user 'uploads' data or changes a setting:

   - *if the whole workflow is trivial, computationally*:
     - this triggers the whole workflow to execute and return with a new state/result immediately, and the output elements reflect the new state without any noticable delay

   - *if only some (or no) components of the workflows are trivial, computationally*:
     - this triggers the execution of only parts of the workflow immediately (from the point of user input to the next non-trivial step of the workflow).
     - all computationally non-trivial parts of the workflow will have some sort of "Process" button that users have to click manually to kick off those parts of the workflow. Otherwise the UI would be locked for an undefined amount of time after every user input -- which would result in a very bad UX).
     - alternatively, workflows with computationally non-trivial parts could have one 'global' "Process" button, which would trigger the execution of the whole workflow with all current inputs/settings.

There will be also inputs that don't directly kick off any processing (like for example control buttons in a visualisation). I
think we can ignore those for now, because this is what UIs usually do, and this does not present a difficulty in terms of
the overall UI/UX (just like the 'computationally trivial' workflow scenario).

### UI representations for the current workflow state

#### tldr;

In some cases it will be impossible for users to use a workflow fully interactively, because one or all workflow steps
will take too much time, which means the interactive session has to be interrupted. In those cases (depending on our setup
and other circumstances) we might need to include a 'job-management'/'queue' component to our application, which matches
running/completed jobs to users and 'sessions'/'experiments' (for lack of a better word).
We need to find a visual metaphors for workflows and workflow steps to make that intuitive, ideally in a way so that those scenarios are not
handled too differently in comparison to how our 100%-interactive workflows are used and executed.
In addition, we have to make sure our backend can deal with all the scenarios we want to support.

#### Details, skip if you want

I'll include some suggestions on how all this could look visually, but those are in no way to be taken as gospel. Just
the most obvious (to me) visual elements to use, which I hope will make it easier to get my point across.
It's probably obvious that the important cases we have to care about are the ones where there is non-trivial computation.
I think we can roughly divide them into 4 categories:

 - *execution time of a few seconds*:
     - in this case a 'spinning-wheel'-progress indidcator is probably enough
     - backend-wise, we (probably) don't have to worry (although, it's not a given this will not crash a hosted app if we have too many users and computations are executed 'in-line')
 - *execution time of a few minutes*:
     - not long enough so that for example a browser session would expire
     - in this case it would be good UX-wise to have a semi-exact progress indicator that either shows a 'done'-percentage, or remaining time
     - on the backend-side, we need to separate three scenarios:
        - local app:
            - the computation can happen locally, either in a new thread, or a different process (we can also make use of multiple cores if available)
        - hosted jupyter in some form or other:
            - the running Jupyter kernel can execute the computation, which is probably a good enough separation to not affect the hosted UI
        - hosted web app:
            - there needs to exist some infrastructure we can use to offload the computation, it can't run on the same host as our service (which means a lot of added complexity)
            - there is no need yet for authentication apart from that we need to be able to assign the result of the computation to individual sessions
 - *execution time of a few hours*:
     - long enough that a user will have left the computer in between, or closed a browser, etc.
     - now the separation of backend-scenarios kicks in earlier, and also affects the front-end:
         - local app:
            - as in the case before, the UI would display a progress-indicator of some sort
            - the computation would happen as a background process, and as long as the user does not shut-down or restart the
              computer there is no issue (the job should even survive a suspend/hibernate)
         - hosted jupyter:
            - difficult to say, the computation could either still happen in the running Jupyter kernel, or would have to be farmed out to an external service
            - one issue to be aware of is that, depending on how it is configured, Jupyter might or might not kill a notebook process (and underlying kernel)
              if there has been no activity in the browser for a while. We'd have to make sure this does not happen, or that we have some sort of user session
              management (which should be entirely possible -- but of course increases complexity by quite a bit). The latter will also be necessary if a user
              comes back to their session after having been disconnected in some way, because otherwise they'd loose their result.
            - ui-wise there needs to be session and compute-job management, and a list of currently running and past jobs and links to the experiments that produced them
         - hosted web app:
            - as with the jupyter case, we'll need session as well as job management
 - *execution time of more than a few hours (days, weeks)*:
     - in all cases the computation now needs to be outsourced, and submitted to a compute service (cloud, HPC, local dask-cluster, whatever...)
     - all cases need to implement some sort of session authentication and job management (which would probably be a bit more transparent to the user in the local case, but overall it would be implemented in a similar way in each scenario)


### Externally running computations

One thing to stress is that 'outsourcing' computationally intensive tasks comes with a considerable amount of complexity.
Nothing that can't be implemented, and there are several ways I can think of to do this. I'd still advise to be very aware of the
cost and complexity this incurs. I do believe we will have to add that in some form at some stage though, if we are in
any way successful and have people adopting our solution. Which means we have to include the issue in our architecture
design, even if we only plan to implement it later.
