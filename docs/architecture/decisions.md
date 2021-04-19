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

## Requiring to subclass an abstract base class when creating a module

The class I'm talking about here is [KiaraModule][kiara.module.KiaraModule]. At it's heart, it's
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
