# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import fnmatch
import re
import typing
from jinja2 import Environment
from pydantic import BaseModel, Field

from kiara.data import Value


class FieldMatcher(BaseModel):
    @classmethod
    def create_default_matchers(cls):

        return [FieldMatcher()]

    match_expr: str = "*"
    matcher_type: str = "glob"
    alias_template: str = "{{ base_id }}__{{ field_name }}"
    check_next_on_match: bool = False


class ValueStoreConfig(BaseModel):
    """A configuration that describes which step outputs of a pipeline to save, and how."""

    @classmethod
    def save_fields(
        self,
        base_id: str,
        value_set: typing.Mapping[str, Value],
        matchers: typing.Optional[typing.List[FieldMatcher]] = None,
    ) -> typing.Dict[Value, typing.List[str]]:

        if matchers is None:
            matchers = FieldMatcher.create_default_matchers()

        for matcher in matchers:
            if matcher.matcher_type not in ["glob", "regex"]:
                raise NotImplementedError(
                    "Only 'glob' and 'regex' field matcher type implemented yet."
                )

            if matcher.matcher_type == "glob":
                matcher.matcher_type = "regex"
                matcher.match_expr = fnmatch.translate(matcher.match_expr)

        env = Environment()

        to_save: typing.Dict[str, typing.Set[str]] = {}
        for field_name in value_set.keys():

            for matcher in matchers:
                if not re.match(matcher.match_expr, field_name):
                    continue

                template = env.from_string(matcher.alias_template)
                rendered = template.render(field_name=field_name, base_id=base_id)

                to_save.setdefault(field_name, set()).add(rendered)
                if not matcher.check_next_on_match:
                    break

        result: typing.Dict[Value, typing.List[str]] = {}
        for field_name, aliases in to_save.items():

            value = value_set[field_name]
            _v = value.save(aliases=aliases)
            result.setdefault(_v, []).extend(aliases)

        return result

    inputs: typing.List[FieldMatcher] = Field(
        description="Whether and how to save inputs.", default_factory=list
    )
    outputs: typing.List[FieldMatcher] = Field(
        description="Whether and how to save inputs.",
        default_factory=FieldMatcher.create_default_matchers,
    )
    steps: typing.List["ValueStoreConfig"] = Field(
        description="Whether and how to save step inputs and outputs.",
        default_factory=list,
    )


ValueStoreConfig.update_forward_refs()
