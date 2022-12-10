## Package content

The *kiara* main package also contains basic, low-level data-types, modules and operations that are necessary
for its core functionality. This page lists all of them.

{% for item_type, item_group in get_context_info().get_all_info().items() %}

### {{ item_type }}
{% for item, details in item_group.item_infos.items() %}
- [`{{ item }}`][kiara_info.{{ item_type }}.{{ item }}]: {{ details.documentation.description }}
{% endfor %}
{% endfor %}
