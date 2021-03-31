import requests

from helpers.requests import create_requests_session


class SurveyCTO:
    # ? When to raise error of wrong credentials
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
        except requests.exceptions.HTTPError as e:
            raise e
        except requests.exceptions.RequestException as e:
            raise e

    def get_all_forms():
        """Get a list of all the forms of the SurveyCTO server
        """
        pass

    def get_form(id):
        """Get a form by its ID

        Args:
            id (string): ID of the form
        """
        pass

    def get_form_submissions(id):
        """Get all submissions of a form

        Args:
            id (string): ID of the form
        """
        pass