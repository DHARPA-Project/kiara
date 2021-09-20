# -*- coding: utf-8 -*-
import click
import os
import sys

from kiara import Kiara
from kiara.utils import log_message


@click.group()
@click.pass_context
def dev(ctx):
    """Development helpers."""


try:

    from kiara_streamlit.defaults import MODULE_DEV_STREAMLIT_FILE  # type: ignore
    from streamlit import bootstrap
    from streamlit.cli import ACCEPTED_FILE_EXTENSIONS, _main_run, configurator_options

    @dev.command("module")
    @configurator_options
    @click.argument("module_name", required=True)
    @click.argument("args", nargs=-1)
    @click.pass_context
    def dev_module(ctx, module_name, args=None, **kwargs):
        """Run a Python script, piping stderr to Streamlit.

        The script can be local or it can be an url. In the latter case, Streamlit
        will download the script to a temporary file and runs this file.

        """

        kiara_obj: Kiara = ctx.obj["kiara"]

        if module_name not in kiara_obj.available_module_types:
            print()
            print(
                f"Can't launch dev UI for module '{module_name}': module not available."
            )
            sys.exit(1)

        m_cls = kiara_obj.get_module_class(module_type=module_name)
        python_module = m_cls.get_type_metadata().python_class.get_module()

        # TODO: some sanity checks
        extra_bit = python_module.__name__.replace(".", os.path.sep) + ".py"
        python_path_to_watch = python_module.__file__[0 : -len(extra_bit)]  # noqa

        _python_path = os.environ.get("PYTHONPATH", None)
        if _python_path is None:
            python_path = []
        else:
            python_path = _python_path.split(":")

        if python_path_to_watch not in python_path:
            python_path.append(python_path_to_watch)
            python_path_export = ":".join(python_path)
            os.environ["PYTHONPATH"] = python_path_export

        os.environ["DEV_MODULE_NAME"] = module_name

        bootstrap.load_config_options(flag_options=kwargs)
        target = MODULE_DEV_STREAMLIT_FILE

        _, extension = os.path.splitext(target)
        if extension[1:] not in ACCEPTED_FILE_EXTENSIONS:
            if extension[1:] == "":
                raise click.BadArgumentUsage(
                    "Streamlit requires raw Python (.py) files, but the provided file has no extension.\nFor more information, please see https://docs.streamlit.io"
                )
            else:
                raise click.BadArgumentUsage(
                    "Streamlit requires raw Python (.py) files, not %s.\nFor more information, please see https://docs.streamlit.io"
                    % extension
                )

        if not os.path.exists(target):
            raise click.BadParameter("File does not exist: {}".format(target))
        _main_run(target, args, flag_options=kwargs)


except Exception:
    log_message("Streamlit not installed, not offering streamlit debug sub-command")
