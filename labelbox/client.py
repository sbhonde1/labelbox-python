from collections import namedtuple
from datetime import datetime, timezone
import json
import logging
import os
import re
import time

import requests
import requests.exceptions

from labelbox import utils
import labelbox.exceptions
from labelbox.orm import query
from labelbox.orm.db_object import DbObject
from labelbox.pagination import PaginatedCollection
from labelbox.schema.project import Project
from labelbox.schema.dataset import Dataset
from labelbox.schema.user import User
from labelbox.schema.organization import Organization
from labelbox.schema.labeling_frontend import LabelingFrontend


logger = logging.getLogger(__name__)


_LABELBOX_API_KEY = "LABELBOX_API_KEY"


class AutoRetry:
    """ Encapsulates info necessary for determining if a query should be
    automatically retried after failure (filter predicates) and how the
    exponential backoff should be performed (maximum retry count and wait-time
    base value).
    """

    class FilterPredicate(namedtuple("FilterPredicate",
                                     "exception_type message_regex params")):
        """
        Evaluates if a given Exception (caught during query execution)
        warrants an execution retry. Composed of three criteria, all of which
        must be satisfied for this predicate to warrant a retry:
            - exception type
            - regex to be matched against the message
            - a dict of key-value pairs to match exception attributes
        A None value passed for any of the criteria means that criteria should
        be ignored.

        Attributes:
            exception_type (type): Class of exception.
            message_regex (str): Regex pattern to match the exception message.
                If present and exception message is None, the predicate fails.
            params (dict): Key-value pairs to match against exception
                attributes. Every key-value pair must be matched for this
                predicate to succeed.
        """
        def matches(self, exception):
            exception_type, message_filter, params = self

            if exception_type and type(exception) != exception_type:
                return False

            if message_filter:
                if exception.message is None:
                    return False
                if re.match(message_filter, exception.message) is None:
                    return False

            if params:
                if not all(getattr(exception, key, None) == value
                           for key, value in params):
                    return False

            return True

    # The default filter predicates
    DEFAULT_FILTER_PREDICATES = [
        FilterPredicate(labelbox.exceptions.BadResponseError,
                        "upstream connect error", {"status_code": 503}),
    ]

    def __init__(self, filter_predicates=DEFAULT_FILTER_PREDICATES,
                 max_retries=5, wait_period_base=4.0):
        """
        Args:
            filter_predicates (iterable of FilterPredicate): Predicates of
                which at least one must succeed (return True) for a retry
                to be attempted.
            max_retries (int): The maximum number of retry attempts.
            wait_period_base (float): The base for the wait period between two
                retries, in milliseconds. The wait period is calculated as:
                wait_period_base ^ retry_attempt_number.
        """
        self.filter_predicates = filter_predicates
        self.max_retries = max_retries
        self.wait_period_base = wait_period_base

    def should_retry(self, exception):
        """ Evalutes if the query should be retried after the given exception
        has been raised.

        Args:
            exception (Exception): The exception that has been raised.
        """
        return any(fp.matches(exception) for fp in self.filter_predicates)


class Client:
    """ A Labelbox client. Contains info necessary for connecting to
    a Labelbox server (URL, authentication key). Provides functions for
    querying and creating top-level data objects (Projects, Datasets).
    """


    def __init__(self, api_key=None,
                 endpoint='https://api.labelbox.com/graphql',
                 auto_retry=AutoRetry()):
        """ Creates and initializes a Labelbox Client.

        Args:
            api_key (str): API key. If None, the key is obtained from
                the "LABELBOX_API_KEY" environment variable.
            endpoint (str): URL of the Labelbox server to connect to.
            auto_retry (AutoRetry): Determines if and how query execution
                should be retried after failure.
        Raises:
            labelbox.exceptions.AuthenticationError: If no `api_key`
                is provided as an argument or via the environment
                variable.
        """
        if api_key is None:
            if _LABELBOX_API_KEY not in os.environ:
                raise labelbox.exceptions.AuthenticationError(
                    "Labelbox API key not provided")
            api_key = os.environ[_LABELBOX_API_KEY]
        self.api_key = api_key
        self.auto_retry = auto_retry

        logger.info("Initializing Labelbox client at '%s'", endpoint)

        self.endpoint = endpoint
        self.headers = {'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer %s' % api_key}

    def execute(self, query, params=None, timeout=10.0):
        """ Sends a request to the server for the execution of the
        given query. Checks the response for errors and wraps errors
        in appropriate labelbox.exceptions.LabelboxError subtypes.

        Performs auto-retries with exponential backoff which is parameterized
        by the `AutoRetry` object passed during `Client` initialization.

        Args:
            query (str): The query to execute.
            params (dict): Query parameters referenced within the query.
            timeout (float): Max allowed time for query execution,
                in seconds.
        Returns:
            dict, parsed JSON response.
        Raises:
            labelbox.exceptions.AuthenticationError: If authentication
                failed.
            labelbox.exceptions.InvalidQueryError: If `query` is not
                syntactically or semantically valid (checked server-side).
            labelbox.exceptions.ApiLimitError: If the server API limit was
                exceeded. See "How to import data" in the online documentation
                to see API limits.
            labelbox.exceptions.TimeoutError: If response was not received
                in `timeout` seconds.
            labelbox.exceptions.NetworkError: If an unknown error occurred
                most likely due to connection issues.
            labelbox.exceptions.LabelboxError: If an unknown error of any
                kind occurred.
        """
        retries = 0
        while True:
            try:
                return self._execute_without_retries(query, params, timeout)
            except Exception as e:
                if retries >= self.auto_retry.max_retries:
                    raise e

                if not self.auto_retry.should_retry(e):
                    raise e

                retries += 1
                time.sleep(self.auto_retry.wait_period_base ** (retries) / 1000)

    def _execute_without_retries(self, query, params=None, timeout=10.0):
        """ Same params like Client.execute. """

        logger.debug("Query: %s, params: %r", query, params)

        # Convert datetimes to UTC strings.
        def convert_value(value):
            if isinstance(value, datetime):
                value = value.astimezone(timezone.utc)
                value = value.strftime("%Y-%m-%dT%H:%M:%SZ")
            return value

        if params is not None:
            params = {key: convert_value(value) for key, value in params.items()}

        data = json.dumps(
            {'query': query, 'variables': params}).encode('utf-8')

        try:
            response = requests.post(self.endpoint, data=data,
                                        headers=self.headers,
                                        timeout=timeout)
            logger.debug("Response: %s", response.text)

        except requests.exceptions.Timeout as e:
            raise labelbox.exceptions.TimeoutError(str(e))

        except requests.exceptions.RequestException as e:
            logger.error("Unknown error: %s", str(e))
            raise labelbox.exceptions.NetworkError(e)

        except Exception as e:
            raise labelbox.exceptions.LabelboxError(
                "Unknown error during Client.query(): " + str(e), e)

        try:
            response = response.json()
        except:
            raise labelbox.exceptions.BadResponseError(
                response.content.decode(), response.status_code)

        errors = response.get("errors", [])

        def check_errors(keywords, *path):
            """ Helper that looks for any of the given `keywords` in any of
            current errors on paths (like error[path][component][to][keyword]).
            """
            for error in errors:
                obj = error
                for path_elem in path:
                    obj = obj.get(path_elem, {})
                if obj in keywords:
                    return error
            return None

        if check_errors(["AUTHENTICATION_ERROR"],
                        "extensions", "exception", "code") is not None:
            raise labelbox.exceptions.AuthenticationError("Invalid API key")

        authorization_error = check_errors(["AUTHORIZATION_ERROR"],
                                           "extensions", "code")
        if authorization_error is not None:
            raise labelbox.exceptions.AuthorizationError(
                authorization_error["message"])

        validation_error = check_errors(["GRAPHQL_VALIDATION_FAILED"],
                                        "extensions", "code")
        if validation_error is not None:
            message = validation_error["message"]
            if message == "Query complexity limit exceeded":
                raise labelbox.exceptions.ValidationFailedError(message)
            else:
                raise labelbox.exceptions.InvalidQueryError(message)

        graphql_error = check_errors(["GRAPHQL_PARSE_FAILED"], "extensions", "code")
        if graphql_error is not None:
            raise labelbox.exceptions.InvalidQueryError(
                graphql_error["message"])

        # Check if API limit was exceeded
        response_msg = response.get("message", "")
        if response_msg.startswith("You have exceeded"):
            raise labelbox.exceptions.ApiLimitError(response_msg)

        if len(errors) > 0:
            logger.warning("Unparsed errors on query execution: %r", errors)
            raise labelbox.exceptions.LabelboxError(
                "Unknown error: %s" % str(errors))

        return response["data"]

    def upload_data(self, data):
        """ Uploads the given data (bytes) to Labelbox.

        Args:
            data (bytes): The data to upload.
        Returns:
            str, the URL of uploaded data.
        Raises:
            labelbox.exceptions.LabelboxError: If upload failed.
        """
        request_data = {
            "operations": json.dumps({
            "variables": {"file": None, "contentLength": len(data), "sign": False},
            "query": """mutation UploadFile($file: Upload!, $contentLength: Int!,
                                            $sign: Boolean) {
                            uploadFile(file: $file, contentLength: $contentLength,
                                       sign: $sign) {url filename} } """,}),
            "map": (None, json.dumps({"1": ["variables.file"]})),
            }
        response = requests.post(
            self.endpoint,
            headers={"authorization": "Bearer %s" % self.api_key},
            data=request_data,
            files={"1": data}
        )

        try:
            file_data = response.json().get("data", None)
        except ValueError as e: # response is not valid JSON
            raise labelbox.exceptions.LabelboxError(
                "Failed to upload, unknown cause", e)

        if not file_data or not file_data.get("uploadFile", None):
            raise labelbox.exceptions.LabelboxError(
                "Failed to upload, message: %s" % file_data.get("error", None))

        return file_data["uploadFile"]["url"]

    def _get_single(self, db_object_type, uid):
        """ Fetches a single object of the given type, for the given ID.

        Args:
            db_object_type (type): DbObject subclass.
            uid (str): Unique ID of the row.
        Returns:
            Object of `db_object_type`.
        Raises:
            labelbox.exceptions.ResourceNotFoundError: If there is no object
                of the given type for the given ID.
        """
        query_str, params = query.get_single(db_object_type, uid)
        res = self.execute(query_str, params)
        res = res[utils.camel_case(db_object_type.type_name())]
        if res is None:
            raise labelbox.exceptions.ResourceNotFoundError(
                db_object_type, params)
        else:
            return db_object_type(self, res)

    def get_project(self, project_id):
        """ Gets a single Project with the given ID.

        Args:
            project_id (str): Unique ID of the Project.
        Returns:
            The sought Project.
        Raises:
            labelbox.exceptions.ResourceNotFoundError: If there is no
                Project with the given ID.
        """
        return self._get_single(Project, project_id)

    def get_dataset(self, dataset_id):
        """ Gets a single Dataset with the given ID.

        Args:
            dataset_id (str): Unique ID of the Dataset.
        Returns:
            The sought Dataset.
        Raises:
            labelbox.exceptions.ResourceNotFoundError: If there is no
                Dataset with the given ID.
        """
        return self._get_single(Dataset, dataset_id)

    def get_user(self):
        """ Gets the current User database object. """
        return self._get_single(User, None)

    def get_organization(self):
        """ Gets the Organization DB object of the current user. """
        return self._get_single(Organization, None)

    def _get_all(self, db_object_type, where):
        """ Fetches all the objects of the given type the user has access to.

        Args:
            db_object_type (type): DbObject subclass.
            where (Comparison, LogicalOperation or None): The `where` clause
                for filtering.
        Returns:
            An iterable of `db_object_type` instances.
        """
        not_deleted = db_object_type.deleted == False
        where = not_deleted if where is None else where & not_deleted
        query_str, params = query.get_all(db_object_type, where)
        return PaginatedCollection(
            self, query_str, params,
            [utils.camel_case(db_object_type.type_name()) + "s"],
            db_object_type)

    def get_projects(self, where=None):
        """ Fetches all the projects the user has access to.

        Args:
            where (Comparison, LogicalOperation or None): The `where` clause
                for filtering.
        Returns:
            An iterable of Projects (typically a PaginatedCollection).
        """
        return self._get_all(Project, where)

    def get_datasets(self, where=None):
        """ Fetches all the datasets the user has access to.

        Args:
            where (Comparison, LogicalOperation or None): The `where` clause
                for filtering.
        Returns:
            An iterable of Datasets (typically a PaginatedCollection).
        """
        return self._get_all(Dataset, where)

    def get_labeling_frontends(self, where=None):
        """ Fetches all the labeling frontends.

        Args:
            where (Comparison, LogicalOperation or None): The `where` clause
                for filtering.
        Returns:
            An iterable of LabelingFrontends (typically a PaginatedCollection).
        """
        return self._get_all(LabelingFrontend, where)

    def _create(self, db_object_type, data):
        """ Creates an object on the server. Attribute values are
            passed as keyword arguments:

        Args:
            db_object_type (type): A DbObjectType subtype.
            data (dict): Keys are attributes or their names (in Python,
                snake-case convention) and values are desired attribute values.
        Returns:
            A new object of the given DB object type.
        Raises:
            InvalidAttributeError: If the DB object type does not contain
                any of the attribute names given in `data`.
        """
        # Convert string attribute names to Field or Relationship objects.
        # Also convert Labelbox object values to their UIDs.
        data = {db_object_type.attribute(attr) if isinstance(attr, str) else attr:
                value.uid if isinstance(value, DbObject) else value
                for attr, value in data.items()}

        query_string, params = query.create(db_object_type, data)
        res = self.execute(query_string, params)
        res = res["create%s" % db_object_type.type_name()]
        return db_object_type(self, res)

    def create_dataset(self, **kwargs):
        """ Creates a Dataset object on the server. Attribute values are
            passed as keyword arguments:
                >>> project = client.get_project("uid_of_my_project")
                >>> dataset = client.create_dataset(name="MyDataset",
                >>>                                 projects=project)

        Kwargs:
            Keyword arguments with new Dataset attribute values.
            Keys are attribute names (in Python, snake-case convention) and
            values are desired attribute values.
        Returns:
            A new Dataset object.
        Raises:
            InvalidAttributeError: If the Dataset type does not contain
                any of the attribute names given in kwargs.
        """
        return self._create(Dataset, kwargs)

    def create_project(self, **kwargs):
        """ Creates a Project object on the server. Attribute values are
            passed as keyword arguments:
                >>> project = client.create_project(name="MyProject")

        Kwargs:
            Keyword arguments with new Project attribute values.
            Keys are attribute names (in Python, snake-case convention) and
            values are desired attribute values.
        Returns:
            A new Project object.
        Raises:
            InvalidAttributeError: If the Project type does not contain
                any of the attribute names given in kwargs.
        """
        return self._create(Project, kwargs)
