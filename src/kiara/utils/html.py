# -*- coding: utf-8 -*-
from airium import Airium
from pydantic import BaseModel
from typing import Any, Iterable, Mapping, Union

from kiara.registries.templates import TemplateRegistry


def generate_html(
    item: Any,
    render_config: Union[None, Mapping[str, Any]] = None,
    add_header: bool = False,
    add_type_column: bool = False,
) -> Airium:
    """Create html representing this models data."""

    doc = Airium()

    if render_config is None:
        render_config = {}
    else:
        render_config = dict(render_config)

    if isinstance(item, str):
        doc(item)
    elif isinstance(item, BaseModel):

        from kiara.models import KiaraModel

        if isinstance(item, KiaraModel):
            template_registry = TemplateRegistry.instance()
            template = template_registry.get_template_for_model_type(
                model_type=item.model_type_id, template_format="html"
            )

            if template:
                rendered = template.render(instance=item)
                doc(rendered)
                return doc

        exclude_fields = None
        model_cls = item.__class__
        props = model_cls.schema().get("properties", {})

        rows = []
        for field_name, field in model_cls.__fields__.items():

            if exclude_fields and field_name in exclude_fields:
                continue

            row = [field_name]

            p = props.get(field_name, None)
            if add_type_column:
                p_type = None
                if p is not None:
                    p_type = p.get("type", None)
                    # TODO: check 'anyOf' keys

                if p_type is None:
                    p_type = "-- check source --"
                row.append(p_type)

            data = getattr(item, field_name)
            row.append(generate_html(data, render_config=render_config))

            desc = p.get("description", "")
            row.append(desc)

            rows.append(row)

        with doc.table():
            if add_header:
                with doc.tr():
                    doc.th(_t="field")
                    if add_type_column:
                        doc.th(_t="type")
                    doc.th(_t="data")
                    doc.th(_t="description")

            for row in rows:
                with doc.tr():
                    doc.td(_t=row[0])
                    doc.td(_t=row[1])
                    doc.td(_t=row[2])
                    if add_type_column:
                        doc.td(_t=row[3])

    elif isinstance(item, Mapping):
        with doc.table():
            for k, v in item.items():
                with doc.tr():
                    doc.td(_t=k)
                    value_el = generate_html(v)
                    doc.td(_t=value_el)
    elif isinstance(item, Iterable):

        with doc.ul():
            for i in item:
                with doc.li():
                    value_el = generate_html(i)
                    doc(str(value_el))

    else:
        doc(str(item))

    return doc
