import re
import logging
from io import StringIO

import requests
from requests.exceptions import HTTPError
import pandas
from requests.exceptions import HTTPError, RequestException

from helpers.requests import create_requests_session


logger = logging.getLogger(__name__)

# TODO very generic utils, move out of here
def csv_to_pandas(csv):
    """
    Parse a CSV string and loads it into a Pandas dataframe
    """
    return pandas.read_csv(StringIO(csv))

class SurveyCTO:
    # ? When to # raise error of wrong credentials
    # TODO get logger
    # TODO remove usaeg of Airflow exceptions
    # TODO to what shape should data be transfered to? Are we using pandas here?
    # TODO encrypted forms?
    def __init__(self, server_name, username=None, password=None):
        self.server_name = server_name
        
        # Get Requests session, build authentication and get CSRF token
        self.session = create_requests_session(debug=False) # ? Can authentication and CSRF token be baked into the session?
        self.auth_basic = requests.auth.HTTPBasicAuth(username, password)
        self.csrf_token = self._get_CSRF_token()

    def _get_CSRF_token(self):
        """Get a CSRF token from the SurveyCTO home page.
        Mainly used for logging in and generating a Python Requests session to make
        authenticated requests.
        """
        try:
            res = self.session.get(f"https://{self.server_name}.surveycto.com/")
            res.raise_for_status()
            csrf_token = re.search(r"var csrfToken = '(.+?)';", res.text).group(1)
            return csrf_token
        except HTTPError as e:
            logger.error('Could not load SurveyCTO landing page for getting the CSRF token')
            logger.error(e)
            raise e
        except Exception as e:
            logger.error('Unexpected error loading SurveyCTO landing page')
            logger.error(e)
            raise e

    def get_all_forms(self):
        """Get a list of all the forms of the SurveyCTO server
        """
        try:
            forms_request = self.session.get(
                f'https://{self.server_name}.surveycto.com/console/forms-groups-datasets/get',
                auth=self.auth_basic,
                headers={
                    "X-csrf-token": self.csrf_token
                }
            )
        except HTTPError as e:
            logger.error('Error getting list of SurveyCTO forms')
            logger.error(e)
            raise e
        except Exception as e:
            logger.error('Unexpected error getting list of SurveyCTO forms')
            logger.error(e)
            raise e
        
        active_forms = [
            form for form in forms_request.json()["forms"] 
            if (
                not form["testForm"] and
                form["deployed"] and
                form["completeSubmissionCount"] > 0
            )
        ]
        return active_forms

    def get_form(self, id):
        """Get a form's details by its ID
        Uses the SurveyCTO internal API

        Args:
            id (string): ID of the form
        """
        try:
            form_details = self.session.get(
                f"https://{self.server_name}.surveycto.com/forms/{id}/workbook/export/load",
                auth=self.auth_basic,
                headers={
                    "X-csrf-token": self.csrf_token
                },
            )
            return form_details.json()
        except HTTPError as e:
            logger.error(f'Error getting details of form of ID: {id}')
            logger.error(e)
            raise e
        except Exception as e:
            logger.error(f'Unexpected error getting details of form of ID: {id}')
            logger.error(e)
            raise e


    def get_form_submissions(self, id):
        """Get all submissions of a form
        This does not get data of repeat groups
        Uses the SurveyCTO CSV import functionality, in the "long format"

        Args:
            id (string): ID of the form
        """

        # TODO How to handle repeat groups?
        # TODO should this return repeat groups?

        url = f"https://{self.server_name}.surveycto.com/api/v1/forms/data/csv/{id}"
        try:
            form_submissions = self.session.get(url, auth=self.auth_basic)
            # TODO handle errors
            # Loading into Pandas
            if form_submissions.text:
                df = csv_to_pandas(form_submissions.text)
                return df
            else: # Form has no submissions
                return pandas.DataFrame()
        except HTTPError as e:
            logger.error(f'Error getting submissions of form of ID: {id}')
            logger.error(e)
            raise e
        except Exception as e:
            logger.error(f'Unexpected error getting submissions of form of ID: {id}')

    def get_repeat_group_submissions(self, form_id, field_name):
        """Get submission of the repeat groups

        Args:
            form_id (string): IF of the form
            field_name (string): Name of the repeat group field
        """
        url = f"https://{self.server_name}.surveycto.com/api/v1/forms/data/csv/{form_id}/{field_name}"
        try:
            repeat_group_submissions = self.session.get(url, auth=self.auth_basic)
        except HTTPError as e:
            logger.error(f'Error getting submissions of form of ID: {id}')
            logger.error(e)
            raise e
        except Exception as e:
            logger.error(f'Unexpected error getting submissions of form of ID: {id}')
            logger.error(e)
            raise e

        if repeat_group_submissions.status_code == 200 and repeat_group_submissions.text:
            df = csv_to_pandas(repeat_group_submissions.text)
            return df
        else: # Form has no submissions. Returning empty dataframe
            return pandas.DataFrame()

    def get_repeat_groups(self, fields):
        """Get the repeat fields of the form
        Mainly used in `get_form_repeat_group_submissions`

        Args:
        """
        repeat_fields = []
        for field in fields:
            if field.get("dataType") == "repeat":
                repeat_fields.append(field)
            if field.get("children"):
                # repeat_fields.append(get_repeat_groups(field.get("children")))
                repeat_fields = repeat_fields + self.get_repeat_groups(field.get("children"))
        return repeat_fields
        