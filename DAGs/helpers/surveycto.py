

class SurveyCTO:
    def __init__(self, surver_name, username=None, password=None):
        self.surver_name = surver_name
        # self.client
        # Fetch the CSRF toke
        self.csrf_token = self._get_CSRF_token()
        # self.session = 

    def _get_CSRF_token():
        """Get a CSRF token from the SurveyCTO home page.
        Mainly used for logging in and generating a Python Requests session to make
        authenticated requests.
        """
        pass

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