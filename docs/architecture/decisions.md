# Decisions

This page lists a few of the main decisions that were taken, what the considerations around them were, their impact, as well as why they were made.

## Using abstract base classes for most of the important business objects

The main abstract base class I'm talking about here is [KiaraModule][kiara.module.KiaraModule]. At it's heart, it's
basically just a wrapper around a pure function, with some utility method describing

Advantages:
- relatively easy to manage 'plugin-like' architecture

Disadvantages:
- more abstraction layers than strictly necessary
