#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from copy import deepcopy
from os.path import dirname, expanduser, join

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.hdxobject import HDXError
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve
from hdx.utilities.state import State

from hdx.scraper.peacesecurity.peacesecurity import PeaceSecurity

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-peacesecurity"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: peacesecurity"


def main(
    save: bool = True,
    use_saved: bool = False,
    err_to_hdx: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.
        err_to_hdx (bool): Whether to write any errors to HDX metadata. Defaults to False.

    Returns:
        None
    """
    with HDXErrorHandler(write_to_hdx=err_to_hdx) as error_handler:
        with State(
            join(dirname(__file__), "dataset_dates.txt"),
            State.dates_str_to_country_date_dict,
            State.country_date_dict_to_dates_str,
        ) as state:
            state_dict = deepcopy(state.get())
            with wheretostart_tempdir_batch(_USER_AGENT_LOOKUP) as info:
                folder = info["folder"]
                with Download() as downloader:
                    retriever = Retrieve(
                        downloader,
                        folder,
                        "saved_data",
                        folder,
                        save,
                        use_saved,
                    )
                    batch = info["batch"]
                    configuration = Configuration.read()
                    peacesecurity = PeaceSecurity(configuration, retriever, error_handler)
                    dataset_names = peacesecurity.get_data(
                        state_dict,
                    )
                    archive_datasets = peacesecurity.check_hdx_datasets()
                    logger.info(f"Number of datasets to archive: {len(archive_datasets)}")
                    for dataset in archive_datasets:
                        try:
                            dataset.update_in_hdx(
                                update_resources=False,
                                hxl_update=False,
                                operation="patch",
                                batch_mode="KEEP_OLD",
                                updated_by_script=_UPDATED_BY_SCRIPT,
                                ignore_fields=[
                                    "resource:description",
                                    "extras",
                                ],
                            )
                        except HDXError:
                            error_handler.add_message(
                                "PeaceSecurity", dataset["name"], "Could not archive"
                            )
                            continue

                    logger.info(f"Number of datasets to upload: {len(dataset_names)}")
                    for _, nextdict in progress_storing_folder(
                        info, dataset_names, "name"
                    ):
                        dataset_name = nextdict["name"]
                        dataset, showcase = peacesecurity.generate_dataset_and_showcase(
                            dataset_name
                        )
                        if not dataset:
                            continue
                        dataset.update_from_yaml(
                            path=join(
                                dirname(__file__),
                                "config",
                                "hdx_dataset_static.yaml",
                            )
                        )
                        dataset["notes"] = dataset["notes"].replace(
                            "\n", "  \n"
                        )  # ensure markdown has line breaks
                        try:
                            dataset.create_in_hdx(
                                remove_additional_resources=True,
                                hxl_update=False,
                                updated_by_script=_UPDATED_BY_SCRIPT,
                                batch=batch,
                                ignore_fields=[
                                    "resource:description",
                                    "extras",
                                ],
                            )
                        except HDXError:
                            error_handler.add_message(
                                "PeaceSecurity", dataset_name, "Could not upload"
                            )
                            continue
                        # if showcase:
                        #     showcase.create_in_hdx()
                        #     showcase.add_dataset(dataset)

            state.set(state_dict)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(
            dirname(__file__), "config", "project_configuration.yaml"
        ),
    )
