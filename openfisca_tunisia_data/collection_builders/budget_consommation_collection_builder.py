#! /usr/bin/env python
# -*- coding: utf-8 -*-


# OpenFisca -- A versatile microsimulation software
# By: OpenFisca Team <contact@openfisca.fr>
#
# Copyright (C) 2011, 2012, 2013, 2014, 2015 OpenFisca Team
# https://github.com/openfisca
#
# This file is part of OpenFisca.
#
# OpenFisca is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# OpenFisca is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


import logging
import os
import pkg_resources

from openfisca_survey_manager.surveys import Survey, SurveyCollection

log = logging.getLogger(__name__)

openfisca_tunisia_data_location = pkg_resources.get_distribution('openfisca-tunisia-data').location
config_files_directory = os.path.join(openfisca_tunisia_data_location)


def build_empty_budget_consommation_survey_collection(years= None):

    if years is None:
        log.error("A list of years to process is needed")

    budget_consommation_survey_collection = SurveyCollection(name = "budget_consommation")
    budget_consommation_survey_collection.set_config_files_directory(config_files_directory)
    input_data_directory = budget_consommation_survey_collection.config.get('data', 'input_directory')
    output_data_directory = budget_consommation_survey_collection.config.get('data', 'output_directory')

    for year in years:
        tables = ["budg0{}".format(i) for i in range(1, 10)] + ["budg{}".format(i) for i in range(10, 21)]
        tables.remove("budg03")
        survey_tables = dict()
        for year in years:
            for table in tables:
                survey_tables[table] = {
                    "stata_file": os.path.join(
                        os.path.dirname(input_data_directory),
                        "budget_consommation",
                        str(year),
                        "stata",
                        "{}.dta".format(table),
                        ),
                    "year": year,
                    }

            survey_name = u"budget_consommation_{}".format(year)
            hdf5_file_path = os.path.join(
                os.path.dirname(output_data_directory),
                u"{}{}".format(survey_name, u".h5")
                )
            print hdf5_file_path
            survey = Survey(
                name = survey_name,
                hdf5_file_path = hdf5_file_path
                )
            for table, table_kwargs in survey_tables.iteritems():
                survey.insert_table(name = table, **table_kwargs)
            surveys = budget_consommation_survey_collection.surveys
            surveys[survey_name] = survey

    return budget_consommation_survey_collection


if __name__ == '__main__':

    budget_consommation_survey_collection = build_empty_budget_consommation_survey_collection(years = [2005])
#    budget_consommation_survey_collection.fill_hdf_from_stata(["budget_consommation_2005"])
    budget_consommation_survey_collection.dump(collection = "budget_consommation")
