#!/usr/bin/python
"""
Unit tests for peacekeeping.

"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.vocabulary import Vocabulary
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from peacesecurity import PeaceSecurity


class TestPeaceSecurity:
    dataset = {
        "data_update_frequency": "1",
        "dataset_date": "[1948-01-01T00:00:00 TO *]",
        "groups": [{"name": "world"}],
        "maintainer": "0d34fa8f-de81-43cc-9c1b-7053455e2e74",
        "name": "peacekeeping-fatalities",
        "notes": "This dataset provides figures on staff and peacekeeper fatalities in Peacekeeping and Special Political Missions from 1948-Present, based on the receipt of official Notifications of Peacekeeper Casualties (NOTICAS).  The dataset specifies details such as casualty mission, casualty nationality and type of incident.",
        "owner_org": "8cb62b36-c3cc-4c7a-aae7-a63e2d480ffc",
        "subnational": "0",
        "tags": [
            {'name': 'complex emergency-conflict-security', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
            {'name': 'fatalities', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
            {'name': 'peacekeeping', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
        ],
        "title": "Peace and Security Pillar: Mission Fatalities - Fatalities in PKOs and SPMs since 1948",
    }
    resource = {
        "description": "",
        "format": "csv",
        "name": "dppadposs-fatalities.csv",
        "resource_type": "file.upload",
        "url_type": "upload",
    }
    showcase = {
        "name": "dppadposs-fatalities-showcase",
        "title": "Peace and Security Pillar: Mission Fatalities - Fatalities in PKOs and SPMs since 1948 Showcase",
        "notes": "This dataset provides figures on staff and peacekeeper fatalities in Peacekeeping and Special Political Missions from 1948-Present, based on the receipt of official Notifications of Peacekeeper Casualties (NOTICAS).  The dataset specifies details such as casualty mission, casualty nationality and type of incident.",
        "url": "https://app.powerbi.com/view?r=eyJrIjoiOTFiYTdhZTktNTA4NC00MWE4LWI4Y2EtMGY4MmY0NmNmOGI5IiwidCI6IjBmOWUzNWRiLTU0NGYtNGY2MC1iZGNjLTVlYTQxNmU2ZGM3MCIsImMiOjh9",
        "image_url": "https://raw.githubusercontent.com/OCHA-DAP/hdx-scraper-peacesecurity/main/config/view_dashboard.jpg",
        "tags": [
            {"name": "complex emergency-conflict-security", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"},
            {"name": "fatalities", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"},
            {"name": "peacekeeping", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"},
        ]
    }

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        UserAgent.set_global("test")
        tags = (
            "complex emergency-conflict-security",
            "peacekeeping",
            "fatalities",
        )
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

    def test_generate_dataset_and_showcase(self, configuration, fixtures):
        with temp_dir(
            "test_peacesecurity", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(downloader, folder, fixtures, folder, False, True)
                peacesecurity = PeaceSecurity(configuration, retriever, folder)
                dataset_names = peacesecurity.get_data(
                    {"DEFAULT": parse_date("2023-01-01")},
                    datasets="DPPADPOSS-FATALITIES",
                )
                assert dataset_names == [{"name": "DPPADPOSS-FATALITIES"}]

                dataset, showcase = peacesecurity.generate_dataset_and_showcase("DPPADPOSS-FATALITIES", configuration)
                assert dataset == self.dataset
                resources = dataset.get_resources()
                assert resources[0] == self.resource
                file = "dppadposs-fatalities.csv"
                assert_files_same(join("tests", "fixtures", file), join(folder, file))
                assert showcase == self.showcase
