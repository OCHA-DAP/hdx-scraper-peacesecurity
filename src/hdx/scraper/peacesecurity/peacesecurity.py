#!/usr/bin/python
"""peacesecurity scraper"""

import logging
from datetime import datetime
from typing import List, Optional

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.downloader import DownloadError
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class PeaceSecurity:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        error_handler: HDXErrorHandler,
    ):
        self.configuration = configuration
        self.retriever = retriever
        self.error_handler = error_handler
        self.dataset_data = {}
        self.metadata = {}
        self.dataset_ids = []

    def get_data(self, datasets: Optional = None) -> List[str]:
        base_url = self.configuration["base_url"]
        meta_url = f"{base_url}metadata/all"
        meta_jsons = self.retriever.download_json(meta_url)

        for meta_json in meta_jsons:
            dataset_id = meta_json["Dataset ID"]
            hdx_dataset_id = self.configuration["dataset_names"].get(
                dataset_id, dataset_id
            )
            hdx_dataset_id = slugify(hdx_dataset_id)
            self.dataset_ids.append(hdx_dataset_id)
            if datasets and dataset_id not in datasets:
                continue
            data_url = f"{base_url}data/{dataset_id}/json"
            try:
                data_json = self.retriever.download_json(data_url)
            except DownloadError:
                self.error_handler.add_message(
                    "PeaceSecurity",
                    dataset_id,
                    "Could not download",
                )
                continue
            self.dataset_data[dataset_id] = data_json
            self.metadata[dataset_id] = meta_json

        return sorted(self.dataset_data)

    def check_hdx_datasets(self) -> List[Dataset]:
        datasets = Dataset.search_in_hdx(fq="organization:unpeacesecurity")
        archive_datasets = []
        for dataset in datasets:
            if dataset["name"] not in self.dataset_ids and not dataset["archived"]:
                dataset["archived"] = True
                archive_datasets.append(dataset)
        return archive_datasets

    def generate_dataset(self, dataset_name) -> Optional[Dataset]:
        rows = self.dataset_data[dataset_name]
        metadata = self.metadata[dataset_name]

        name = self.configuration["dataset_names"].get(
            dataset_name, metadata["Dataset ID"]
        )
        title = f"Peace and Security Pillar: {metadata['Name']}"
        words = title.split(" ")
        for i, word in enumerate(words):
            upper_case = sum(1 for c in word if c.isupper())
            if upper_case > 1 or word in ["and", "by", "in", "for", "of", "the", "to"]:
                continue
            words[i] = word.title()
        title = " ".join(words)
        dataset = Dataset({"name": slugify(name), "title": title})
        dataset.set_maintainer("0d34fa8f-de81-43cc-9c1b-7053455e2e74")
        dataset.set_organization("8cb62b36-c3cc-4c7a-aae7-a63e2d480ffc")
        update_frequency = metadata["Update Frequency"]
        if update_frequency.lower() == "ad hoc":
            update_frequency = "adhoc"
        dataset.set_expected_update_frequency(update_frequency)
        dataset.set_subnational(False)
        try:
            countryiso3, exact = Country.get_iso3_country_code_fuzzy(metadata["Name"])
        except KeyError:
            logger.error(f"Could not get iso code from name: {metadata['Name']}")
            countryiso3 = None
            exact = False
        if countryiso3 and (countryiso3 != "GBR" and not exact):
            dataset.add_country_location(countryiso3)
        else:
            dataset.add_other_location("world")
        dataset["notes"] = metadata["Description"]
        filename = f"{dataset_name.lower()}.csv"
        resourcedata = {
            "name": filename,
            "description": "",
        }
        tags = set()
        tags.add("complex emergency-conflict-security")
        tags.add("peacekeeping")
        if metadata["Tags"]:
            for tag in metadata["Tags"]:
                tags.add(tag["Tag"].lower())
        if metadata["Themes"]:
            for theme in metadata["Themes"]:
                tags.add(theme["Theme"].lower())
        tags = sorted([t for t in tags if t in self.configuration["allowed_tags"]])
        dataset.add_tags(tags)

        headers = rows[0].keys()
        date_headers = [
            h for h in headers if "date" in h.lower() and isinstance(rows[0][h], int)
        ]
        dates = []
        for row in rows:
            for date_header in date_headers:
                row_date = row[date_header]
                if not row_date:
                    continue
                if len(str(row_date)) > 9:
                    row_date = row_date / 1000
                row_date = datetime.utcfromtimestamp(row_date)
                dates.append(row_date)
                row_date = row_date.strftime("%Y-%m-%d")
                row[date_header] = row_date

        if len(dates) > 0:
            start_date = min(dates)
            end_date = max(dates)
            ongoing = False
        else:
            start_date = metadata["Start Range"]
            end_date = metadata["End Range"]
            ongoing = True
            if end_date:
                ongoing = False
        if not start_date:
            self.error_handler.add_message(
                "PeaceSecurity",
                dataset_name,
                "Start date missing",
            )
            return None
        dataset.set_time_period(start_date, end_date, ongoing)

        dataset.generate_resource_from_rows(
            self.retriever.temp_dir,
            filename,
            rows,
            resourcedata,
            list(rows[0].keys()),
        )

        return dataset
