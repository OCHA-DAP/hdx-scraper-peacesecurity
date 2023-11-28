#!/usr/bin/python
"""
Peace and Security:
------------

Reads Peace and Security JSONs and creates datasets.

"""
import logging
from datetime import datetime

from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from slugify import slugify

logger = logging.getLogger(__name__)


class PeaceSecurity:
    def __init__(self, configuration, retriever, folder):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.dataset_data = {}
        self.metadata = {}

    def get_data(self):
        base_url = self.configuration["base_url"]
        datasets = self.configuration["datasets"]

        for dataset_name in datasets:
            data_url = f"{base_url}data/{dataset_name}/json"
            meta_url = f"{base_url}metadata/{dataset_name}"
            if dataset_name == "DPPADPOSS-PKO":
                data_url = f"{data_url}/?filter=mission_isactive:Yes"

            data_json = self.retriever.download_json(data_url)
            meta_json = self.retriever.download_json(meta_url)

            self.dataset_data[dataset_name] = data_json
            self.metadata[dataset_name] = meta_json[0]

        return [{"name": dataset_name} for dataset_name in sorted(self.dataset_data)]

    def generate_dataset_and_showcase(self, dataset_name):
        rows = self.dataset_data[dataset_name]
        metadata = self.metadata[dataset_name]

        name = metadata["Dataset ID"]
        title = f"Peace and Security Pillar: {metadata['Name']}"
        dataset = Dataset({"name": slugify(name).lower(), "title": title})
        dataset.set_maintainer("0d34fa8f-de81-43cc-9c1b-7053455e2e74")
        dataset.set_organization("8cb62b36-c3cc-4c7a-aae7-a63e2d480ffc")
        update_frequency = metadata["Update Frequency"]
        if update_frequency.lower() == "ad hoc":
            update_frequency = "adhoc"
        dataset.set_expected_update_frequency(update_frequency)
        dataset.set_subnational(False)
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
        for tag in metadata["Tags"]:
            tags.add(tag["Tag"].lower())
        dataset.add_tags(tags)

        start_date = metadata["Start Range"]
        end_date = metadata["End Range"]
        ongoing = True
        if end_date:
            ongoing = False
        if not start_date:
            start_date = datetime.today()
        dataset.set_reference_period(start_date, end_date, ongoing)

        for row in rows:
            dates = self.configuration["dataset_dates"][dataset_name]
            for date in dates:
                row_date = row[date]
                if not row_date:
                    continue
                if len(str(row_date)) > 9:
                    row_date = row_date / 1000
                row_date = datetime.utcfromtimestamp(row_date)
                row_date = row_date.strftime("%Y-%m-%d")
                row[date] = row_date

        dataset.generate_resource_from_rows(
            self.folder,
            filename,
            rows,
            resourcedata,
            list(rows[0].keys()),
        )

        if not metadata["Visualization Link"]:
            return dataset, None

        showcase = Showcase(
            {
                "name": f"{dataset['name']}-showcase",
                "title": f"{dataset['title']} Showcase",
                "notes": dataset["notes"],
                "url": metadata["Visualization Link"],
                "image_url": "https://data.humdata.org/image/2018-03-26-152458.335430United-Nations-Peacekeeping-whitebg.jpg",
            }
        )
        showcase.add_tags(tags)

        return dataset, showcase
