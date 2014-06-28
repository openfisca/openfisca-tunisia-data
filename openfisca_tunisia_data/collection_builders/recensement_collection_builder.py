#! /usr/bin/env python
# -*- coding: utf-8 -*-


# OpenFisca -- A versatile microsimulation software
# By: OpenFisca Team <contact@openfisca.fr>
#
# Copyright (C) 2011, 2012, 2013, 2014 OpenFisca Team
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


def build_empty_recensement_survey_collection(years= None):

    if years is None:
        log.error("A list of years to process is needed")

    recensement_survey_collection = SurveyCollection(name = "recensement")
    recensement_survey_collection.set_config_files_directory(config_files_directory)
    input_data_directory = recensement_survey_collection.config.get('data', 'input_directory')
    output_data_directory = recensement_survey_collection.config.get('data', 'output_directory')

    for year in years:
        tables = [
#            "LOGMENT_ECH_{}".format(year), # TODO: produces a strange error
            "individu_ech_{}".format(year),
            "MENAG_ECH_{}".format(year),
            ]
        survey_tables = dict()
        for year in years:
            for table in tables:
                survey_tables[table] = {
                    "spss_file": os.path.join(
                        os.path.dirname(input_data_directory),
                        "recensement",
                        str(year),
                        "{}.sav".format(table),
                        ),
                    "year": year,
                    }
            survey_name = u"recensement_{}".format(year)
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
            surveys = recensement_survey_collection.surveys
            surveys[survey_name] = survey

    return recensement_survey_collection


if __name__ == '__main__':
    years = [2004]
    recensement_survey_collection = build_empty_recensement_survey_collection(years = years)
    for year in years:
        recensement_survey_collection.fill_hdf_from_spss(["recensement_{}".format(year)])
    recensement_survey_collection.dump(collection = "recensement")
