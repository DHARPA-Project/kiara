# Decisions

This page lists a few of the main decisions that were taken, what the considerations around them were, their impact, as well as why they were made.

## Supporting two sorts of modules: 'core', and 'pipeline' modules

When starting to write code, we didn't have yet many examples of modules and how specific/broad they would be in their utility.
I think we should have done a better job gathering those and coming up with a set of modules that would be sufficient for
our first 10 or so workflows before writing any code, but alas, we didn't. One way I saw to lower the risk of us implementing
us ourselves into a corner was to make our modules as flexible, re-usable and 'combinable' as possible. And the best way
I could think of to do that was to have a very simple interface for each module (each module has a defined set of input/output fields, and one main function to transform inputs into outputs), and to allow several modules to be combined into a 'new'
module that has those same characteristics/interface.

Advantages:
- easy to declaratively create re-usable modules with just json/yaml
- easy to re-use modules/other pipelines for UI-specific subtasks (like data previews/querying)
- in most cases, the higher-level backend code does not know about core- and pipeline- modules, since they can be treated the same

Disadvantages:
- the lower-level backend code needs to implement two different ways to assemble/create modules, depending on whether it's a core-module, or a pipeline


## Use of subclassing in general

Across *kiara*, I'm using subclassing and inheritance in some instances, esp. important base classes are [KiaraModule][kiara.module.KiaraModule] and [PipelineController][kiara.pipeline.controller.PipelineController]. I'm aware that this is considered bad practice in a lot of cases, and I have [read](https://www.sicpers.info/2018/03/why-inheritance-never-made-any-sense/) [my](https://python-patterns.guide/gang-of-four/composition-over-inheritance/) [share](https://python-patterns.guide/gang-of-four/composition-over-inheritance/#problem-the-subclass-explosion) of opinions and thoughts about the matter. In principle I agree, and I'm not 100% happy with every decision I made (or thought I had to made) in this area for *kiara*, but overall I decided to allow for some inheritance and class-based code sharing in the code, partly to speed up my implementation work, partly because I thought some of the disadvantages (like having to search base classes for some function definitions) are not as bad in a certain context than in others. I can totally see how others would disagree here, though, and there are a few things I would like to change/improve later on, if I find the time.

One of the main advantages I get out of using inheritance is being able to automatically discover subclasses of a base class. This is done for multiple of those, like:

- [KiaraModule][kiara.module.KiaraModule]
- [ValueTypeOrm][kiara.data.type.ValueTypeOrm]
- [MetadataModel][kiara.metadata.MetadataModel]

Using auto-discovery in a Python virtualenv removes the need for workflow/module developers to understand Python packaging and entry_points. I've written a [project template](https://github.com/DHARPA-Project/kiara_modules.project_template) that sets up all the basics, and developers focus on creating new classes (basically plugins), with no extra registration work to be done. I hope this will aid adoption. And that I've managed to design those base classes well enough so that they are easy to use and understand, so that some of the main drawbacks of subclassing won't matter all that much.


## Requiring to subclass an abstract base class when creating a module

The main class that uses a subclassing-strategy is [KiaraModule][kiara.module.KiaraModule]. At it's heart, it's
basically just a wrapper around a pure function, with some utility methods describing it's input and output. One reason
I decided to not just create a decorator that wraps any function was the need to be able to describe the input the
function takes, and the output it produces in a stricter way than would have been possible with just type hints.
Another reason is that this way it is possible to add configuration to a module object, which should make module
code much more flexible and re-usable, and developers do not have to implement separate modules for just slightly different
use-cases.

This design decision does not prevent to allow for more 'loose' implementations of a module, like the above mentioned
function with a decorator. Those would be dynamically converted into a ``KiaraModule`` subclass/object, with potential
downsides of not being able to version control it properly (or as easliy). The point is, though, that the default
way of doing things will give us the best guarantees (and metadata).

Advantages:
- relatively easy to manage 'plugin-like' architecture, discovery of modules
- being able to describe module input/output fields in detail
- module versioning: by requiring the subclassing of a base class, and also having to add modules as entry_points, it will be possible describe exactly which version of the module was used in a workflow (as well as which version of the base class)

Disadvantages:
- more abstraction layers than strictly necessary
- other, usual disadvantages associated with subclassing/inheritance


## Separating data from the Python objects that describe them / Data registry

TBD

Advantages:
- efficiency, option to save on memory and IO
- (hopefully) decrease of complexity for non trivial scenarios like multi-process or remote job execution

Disadvantages:
- extra level of abstraction
- increase in complexity (at least for simple use-cases)
