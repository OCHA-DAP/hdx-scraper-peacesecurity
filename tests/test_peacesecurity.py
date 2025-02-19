from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.peacesecurity.peacesecurity import PeaceSecurity


class TestPeaceSecurity:
    dataset = {
        "name": "peacekeeping-fatalities",
        "title": "Peace and Security Pillar: Mission Fatalities - Fatalities in PKOs and "
        "SPMs since 1948",
        "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
        "owner_org": "8cb62b36-c3cc-4c7a-aae7-a63e2d480ffc",
        "data_update_frequency": "1",
        "subnational": "0",
        "groups": [{"name": "world"}],
        "notes": "This dataset provides figures on staff and peacekeeper fatalities in "
        "Peacekeeping and Special Political Missions from 1948-Present, based on the "
        "receipt of official Notifications of Peacekeeper Casualties (NOTICAS).  The "
        "dataset specifies details such as casualty mission, casualty nationality and "
        "type of incident.",
        "tags": [
            {
                "name": "complex emergency-conflict-security",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "fatalities",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "peacekeeping",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
        ],
        "dataset_date": "[1948-01-01T00:00:00 TO *]",
        "license_id": "cc-by-igo",
        "methodology": "Registry",
        "dataset_source": "Peace and Security Pillar",
        "package_creator": "HDX Data Systems Team",
        "private": False,
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
        "title": "Peace and Security Pillar: Mission Fatalities - Fatalities in PKOs and "
        "SPMs since 1948 Showcase",
        "notes": "This dataset provides figures on staff and peacekeeper fatalities in "
        "Peacekeeping and Special Political Missions from 1948-Present, based on the "
        "receipt of official Notifications of Peacekeeper Casualties (NOTICAS).  The "
        "dataset specifies details such as casualty mission, casualty nationality and "
        "type of incident.",
        "url": "https://app.powerbi.com/view?r=eyJrIjoiOTFiYTdhZTktNTA4NC00MWE4LWI4Y2EtMGY4MmY0NmNmOGI5IiwidCI6IjBmOWUzNWRiLTU0NGYtNGY2MC1iZGNjLTVlYTQxNmU2ZGM3MCIsImMiOjh9",
        "image_url": "https://raw.githubusercontent.com/OCHA-DAP/hdx-scraper-peacesecurity/main/config/view_dashboard.jpg",
        "tags": [
            {
                "name": "complex emergency-conflict-security",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "fatalities",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "peacekeeping",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
        ],
    }

    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "peacesecurity", "config")

    def test_peacesecurity(self, configuration, fixtures_dir, input_dir, config_dir):
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "test_peacesecurity",
                delete_on_success=True,
                delete_on_failure=False,
            ) as tempdir:
                with Download(user_agent="test") as downloader:
                    retriever = Retrieve(
                        downloader=downloader,
                        fallback_dir=tempdir,
                        saved_dir=input_dir,
                        temp_dir=tempdir,
                        save=False,
                        use_saved=True,
                    )
                    peacesecurity = PeaceSecurity(configuration, retriever, error_handler)
                    dataset_names = peacesecurity.get_data(
                        {"DEFAULT": parse_date("2023-01-01")},
                        datasets="DPPADPOSS-FATALITIES",
                    )
                    assert dataset_names == [{"name": "DPPADPOSS-FATALITIES"}]

                    dataset, showcase = peacesecurity.generate_dataset_and_showcase(
                        "DPPADPOSS-FATALITIES"
                    )
                    dataset.update_from_yaml(
                        path=join(config_dir, "hdx_dataset_static.yaml")
                    )
                    assert dataset == self.dataset
                    resources = dataset.get_resources()
                    assert resources[0] == self.resource
                    file = "dppadposs-fatalities.csv"
                    assert_files_same(join(input_dir, file), join(tempdir, file))
                    assert showcase == self.showcase
