## Create a module that renders a value of a custom data type

When you create a new data type, by default *kiara* does not know how to render it for specific target(s) (html, terminal, ...). Which means you'll have to create a module for each of the targets you want to support. *kiara* uses the custom `render_value` operation type for this.

There are multiple ways to implement support for rendering a new data type, the easiest one is to add a method with the following signature to the data type class:

```python
    def render_as__<target_type>(
        self, value: "Value", render_config: Mapping[str, Any], manifest: "Manifest"
    ) -> <target_type_cls>:
        ...
        ...
```

So, to implement terminal rendering, that would be:

```python
    def render_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any], manifest: "Manifest"
    ) -> RenderableType:
        ...
        ...
```

As an example, here's how to implement basic rendering of a 'dict' value:
