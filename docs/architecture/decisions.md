# Decisions

This page lists a few of the main decisions that were taken, what the considerations around them were, their impact, as well as why they were made.

## Requiring to subclass an abstract base class when creating a module

The class I'm talking about here is [KiaraModule][kiara.module.KiaraModule]. At it's heart, it's
basically just a wrapper around a pure function, with some utility methods describing it's input and output. One reason
I decided to not just create a decorator that wraps any function was the need to be able to describe the input the
function takes, and the output it produces

Advantages:
- relatively easy to manage 'plugin-like' architecture

Disadvantages:
- more abstraction layers than strictly necessary
