#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from copy import deepcopy
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from hdx.utilities.retriever import Retrieve
from hdx.utilities.state import State

from peacesecurity import PeaceSecurity, update_state

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-peacesecurity"
updated_by_script = "HDX Scraper: Peace and Security"


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and create them in HDX"""
    with ErrorsOnExit() as errors:
        with State(
            "dataset_dates.txt",
            State.dates_str_to_country_date_dict,
            State.country_date_dict_to_dates_str,
        ) as state:
            state_dict = deepcopy(state.get())
            with wheretostart_tempdir_batch(lookup) as info:
                folder = info["folder"]
                with Download() as downloader:
                    open("errors.txt", "w").close()
                    retriever = Retrieve(
                        downloader, folder, "saved_data", folder, save, use_saved
                    )
                    folder = info["folder"]
                    batch = info["batch"]
                    configuration = Configuration.read()
                    peacesecurity = PeaceSecurity(configuration, retriever, folder, errors)
                    dataset_names = peacesecurity.get_data(
                        state_dict,
                    )
                    logger.info(f"Number of datasets to upload: {len(dataset_names)}")

                    for _, nextdict in progress_storing_folder(info, dataset_names, "name"):
                        dataset_name = nextdict["name"]
                        dataset, showcase = peacesecurity.generate_dataset_and_showcase(dataset_name)
                        if dataset:
                            dataset.update_from_yaml()
                            dataset["notes"] = dataset["notes"].replace(
                                "\n", "  \n"
                            )  # ensure markdown has line breaks
                            try:
                                dataset.create_in_hdx(
                                    remove_additional_resources=True,
                                    hxl_update=False,
                                    updated_by_script=updated_by_script,
                                    batch=batch,
                                    ignore_fields=["resource:description", "extras"],
                                )
                            except HDXError:
                                errors.add(f"Could not upload {dataset_name}")
                                continue
                            # if showcase:
                            #     showcase.create_in_hdx()
                            #     showcase.add_dataset(dataset)

                if len(errors.errors) > 0:
                    with open("errors.txt", "w") as fp:
                        fp.write("\n".join(errors.errors))
                    state_dict = update_state(state_dict, state, errors)
            state.set(state_dict)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml")
    )
