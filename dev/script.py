# -*- coding: utf-8 -*-
# %%


from kiara.utils.cli import terminal_print

step_read_files_in_folder = Step(
    "onboarding.folder.import", step_id="read_files_in_folder"
)


step_create_table_from_csvs_config = {
    "columns": ["id", "rel_path", "file_name", "content"]
}
step_create_table_from_csvs = Step(
    "table.from_file_bundle",
    module_config=step_create_table_from_csvs_config,
    step_id="create_table_from_csvs",
)


step_extract_date_from_file_name_config = {"module_type": "date.extract_from_string"}
step_extract_date_from_file_name = Step(
    "array.map",
    module_config=step_extract_date_from_file_name_config,
    step_id="extract_date_from_file_name",
)


step_extract_ref_from_file_name_config = {
    "module_type": "string.match_regex",
    "module_config": {
        "regex": "(\\w+\\d+)_\\d{4}-\\d{2}-\\d{2}_",
        "only_first_match": True,
    },
}
step_extract_ref_from_file_name = Step(
    "array.map",
    module_config=step_extract_ref_from_file_name_config,
    step_id="extract_ref_from_file_name",
)


step_lookup_publication_name_config = {
    "module_type": "string.replace",
    "module_config": {
        "replacement_map": {
            "sn85066408": "L\\'Italia",
            "2012271201": "Cronaca Sovversiva",
        }
    },
}
step_lookup_publication_name = Step(
    "array.map",
    module_config=step_lookup_publication_name_config,
    step_id="lookup_publication_name",
)


step_create_date_range_filter_config = {
    "module_type": "date.range_check",
    "input_name": "date",
}
step_create_date_range_filter = Step(
    "array.map",
    module_config=step_create_date_range_filter_config,
    step_id="create_date_range_filter",
)


step_merged_table = Step("table.merge", step_id="merged_table")


step_filtered_table = Step("table.filter.with_mask", step_id="filtered_table")


step_tokenize_text_corpus_config = {
    "module_type": "language.tokens.tokenize_text",
    "input_name": "text",
}
step_tokenize_text_corpus = Step(
    "array.map",
    module_config=step_tokenize_text_corpus_config,
    step_id="tokenize_text_corpus",
)


step_remove_stopwords = Step(
    "language.tokens.remove_stopwords", step_id="remove_stopwords"
)


step_lemmatize_corpus = Step(
    "language.lemmatize.tokens_array", step_id="lemmatize_corpus"
)


step_generate_lda = Step("language.lda.LDA", step_id="generate_lda")

step_create_table_from_csvs.input.files = step_read_files_in_folder.output.file_bundle


step_extract_date_from_file_name.input.array = (
    step_create_table_from_csvs.output.table.file_name
)


step_extract_ref_from_file_name.input.array = (
    step_create_table_from_csvs.output.table.file_name
)


step_lookup_publication_name.input.array = step_extract_ref_from_file_name.output.array


step_create_date_range_filter.input.array = (
    step_extract_date_from_file_name.output.array
)


step_merged_table.input.sources = [
    step_create_table_from_csvs.output.table,
    step_extract_date_from_file_name.output.array,
    step_extract_ref_from_file_name.output.array,
    step_lookup_publication_name.output.array,
]


step_filtered_table.input.table = step_merged_table.output.table

step_filtered_table.input.mask = step_create_date_range_filter.output.array


step_tokenize_text_corpus.input.array = step_filtered_table.output.table.content


step_remove_stopwords.input.token_lists = step_tokenize_text_corpus.output.array


step_lemmatize_corpus.input.tokens_array = step_remove_stopwords.output.token_list


step_generate_lda.input.tokens_array = step_lemmatize_corpus.output.tokens_array


workflow = step_generate_lda.workflow

step_read_files_in_folder.input.path = (
    "/home/markus/projects/dharpa/notebooks/TopicModelling/data_tm_workflow"
)
step_create_date_range_filter.input.earliest = "1919-01-01"
step_create_date_range_filter.input.latest = "2000-01-01"
step_remove_stopwords.input.languages = ["italian", "german"]
step_generate_lda.input.compute_coherence = True
workflow.process()
terminal_print(workflow)
